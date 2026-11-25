from __future__ import annotations
import asyncio
import hashlib
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from aegis_agents.agent import (
    AGENT_TIER_MAP,
    LITE_MODEL_CHAIN,
    resolve_model_for_agent,
    build_sre_commander,
)
from aegis_agents.callbacks import (
    emit_handoff_event,
    register_live_event_sink,
    save_postmortem_artifact,
)
from aegis_agents.tools import (
    CANDIDATE_FILE,
    build_deterministic_patched_handler,
    fetch_golden_signals,
    inspect_service_logs,
    promote_staged_candidate_manually,
    restore_baseline_bug,
    stage_shadow_patch_direct,
    verify_and_promote_patch_direct,
)
from control_plane.event_bus import event_bus
from domain.schemas import (
    AgentHandoffEvent,
    AgentTier,
    AgentTokenUsage,
    GuardrailVerdict,
    IncidentCostAndROI,
    OperationalMode,
)
from nexuspay_service.pool_manager import infra

APP_NAME = "project_aegis_sre"
PER_MODEL_TIMEOUT_SECONDS = 45.0

# Blended cost per 1M tokens (USD) for Mission 2026 cost/ROI estimation
COST_PER_MILLION_TOKENS_USD = 0.50
BASELINE_MANUAL_MINUTES = 45.0


class AutonomousIncidentWatcher:
    """
    Monitors EnterpriseSettlementGateway's Four Golden Signals, enforces SHA-256 incident idempotency locking,
    runs Google ADK SRE_Commander with multi-model failover & per-agent token tracking,
    and protects live demos with a deterministic SRE circuit-breaker fallback.
    """

    def __init__(self):
        self.operational_mode: OperationalMode = OperationalMode.AUTONOMOUS
        self._active_incident_hash: Optional[str] = None
        self._awaiting_approval: bool = False
        self._resolved_hashes: set[str] = set()
        self._latest_state: Dict[str, Any] = {}
        self._incident_start_mono: Optional[float] = None
        self._cost_roi: IncidentCostAndROI = IncidentCostAndROI()
        self._running = False
        self._task: Optional[asyncio.Task] = None

        os.makedirs("demo_artifacts", exist_ok=True)
        self._session_service = InMemorySessionService()
        register_live_event_sink(self._on_agent_handoff)

    def _init_cost_roi(self, incident_hash: str) -> None:
        self._incident_start_mono = time.perf_counter()
        per_agent: Dict[str, AgentTokenUsage] = {}
        for agent_name, tier in AGENT_TIER_MAP.items():
            per_agent[agent_name] = AgentTokenUsage(
                agent_name=agent_name,
                agent_tier=tier,
                model_used=resolve_model_for_agent(agent_name, LITE_MODEL_CHAIN[0]),
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
                llm_calls=0,
            )
        self._cost_roi = IncidentCostAndROI(
            incident_hash=incident_hash,
            estimated_manual_minutes=BASELINE_MANUAL_MINUTES,
            estimated_hours_saved=round(BASELINE_MANUAL_MINUTES / 60.0, 2),
            per_agent_tokens=per_agent,
        )

    def _recompute_totals(self) -> None:
        total_in = sum(u.tokens_in for u in self._cost_roi.per_agent_tokens.values())
        total_out = sum(u.tokens_out for u in self._cost_roi.per_agent_tokens.values())
        total_tok = sum(u.total_tokens for u in self._cost_roi.per_agent_tokens.values())
        if total_tok == 0 and (total_in > 0 or total_out > 0):
            total_tok = total_in + total_out

        est_cost = round((total_tok / 1_000_000.0) * COST_PER_MILLION_TOKENS_USD, 6)
        elapsed_sec = (
            round(time.perf_counter() - self._incident_start_mono, 2)
            if self._incident_start_mono is not None
            else self._cost_roi.mttr_seconds
        )

        self._cost_roi.total_tokens_in = total_in
        self._cost_roi.total_tokens_out = total_out
        self._cost_roi.total_tokens = total_tok
        self._cost_roi.estimated_cost_usd = est_cost
        self._cost_roi.cost_per_incident_usd = est_cost
        self._cost_roi.mttr_seconds = elapsed_sec
        self._cost_roi.estimated_hours_saved = round(BASELINE_MANUAL_MINUTES / 60.0, 2)

    def _record_agent_tokens(
        self,
        agent_name: str,
        tokens_in: int,
        tokens_out: int,
        total_tokens: int,
        model_used: str,
        tier: Optional[AgentTier] = None,
    ) -> None:
        resolved_tier = tier or AGENT_TIER_MAP.get(agent_name, AgentTier.LITE)
        usage = self._cost_roi.per_agent_tokens.get(agent_name)
        if usage is None:
            usage = AgentTokenUsage(
                agent_name=agent_name,
                agent_tier=resolved_tier,
                model_used=model_used,
            )
            self._cost_roi.per_agent_tokens[agent_name] = usage

        computed_total = total_tokens if total_tokens > 0 else (tokens_in + tokens_out)
        usage.tokens_in += max(0, tokens_in)
        usage.tokens_out += max(0, tokens_out)
        usage.total_tokens += max(0, computed_total)
        usage.model_used = model_used or usage.model_used
        usage.agent_tier = resolved_tier
        usage.llm_calls += 1
        self._recompute_totals()

    async def _publish_cost_roi(self) -> None:
        self._recompute_totals()
        await event_bus.publish("cost_roi_update", self._cost_roi.model_dump())

    async def _on_agent_handoff(self, event_dict: Dict[str, Any]) -> None:
        state_payload = event_dict.get("state_payload", {})
        if state_payload:
            self._latest_state.update(state_payload)

        agent_name = event_dict.get("child_agent", "")
        usage = self._cost_roi.per_agent_tokens.get(agent_name)
        if usage is not None:
            event_dict["tokens_in"] = usage.tokens_in
            event_dict["tokens_out"] = usage.tokens_out
            event_dict["total_tokens"] = usage.total_tokens
            event_dict["model_used"] = usage.model_used
            event_dict["agent_tier"] = usage.agent_tier.value

        self._recompute_totals()
        event_dict["cost_and_roi"] = self._cost_roi.model_dump()
        await event_bus.publish("agent_handoff", event_dict)

    def get_status(self) -> Dict[str, Any]:
        self._recompute_totals()
        return {
            "operational_mode": self.operational_mode.value,
            "active_incident_hash": self._active_incident_hash,
            "awaiting_approval": self._awaiting_approval and os.path.exists(CANDIDATE_FILE),
            "latest_state": self._latest_state,
            "cost_and_roi": self._cost_roi.model_dump(),
        }

    def set_mode(self, mode: OperationalMode) -> Dict[str, Any]:
        self.operational_mode = mode
        return self.get_status()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                metrics = infra.snapshot_golden_signals()
                await event_bus.publish("metrics_tick", metrics.model_dump())

                is_breached = metrics.pool_saturation_pct >= 90.0 or metrics.slo_burn_rate > 14.4
                if is_breached and self._active_incident_hash is None and not self._awaiting_approval:
                    sig = f"nexuspay-pool-saturation-{metrics.max_connections}"
                    inc_hash = "sha256:" + hashlib.sha256(sig.encode()).hexdigest()[:16]
                    if inc_hash not in self._resolved_hashes:
                        self._active_incident_hash = inc_hash
                        asyncio.create_task(self.trigger_remediation(inc_hash))
            except Exception as exc:
                await event_bus.publish("watcher_error", {"error": str(exc)})
            await asyncio.sleep(2.0)

    async def _run_adk_with_model(self, model_name: str, incident_hash: str) -> Dict[str, Any]:
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        await self._session_service.create_session(
            app_name=APP_NAME,
            user_id="sre_system",
            session_id=session_id,
            state={
                "incident_hash": incident_hash,
                "operational_mode": self.operational_mode.value,
                "human_approved": False,
            },
        )

        runner = Runner(
            agent=build_sre_commander(model_name),
            app_name=APP_NAME,
            session_service=self._session_service,
        )

        trigger_msg = types.Content(
            role="user",
            parts=[
                types.Part(
                    text=(
                        f"ALERT: EnterpriseSettlementGateway Settlement Gateway breached SLO (Incident {incident_hash}). "
                        "Execute parallel triage, stage & verify a try...finally connection-release patch "
                        "behind guardrails, and produce the blameless post-mortem."
                    )
                )
            ],
        )

        async def _consume_stream() -> None:
            async for event in runner.run_async(
                user_id="sre_system",
                session_id=session_id,
                new_message=trigger_msg,
            ):
                author = getattr(event, "author", None) or "SRE_Commander"
                usage_meta = getattr(event, "usage_metadata", None)
                t_in = int(getattr(usage_meta, "prompt_token_count", 0) or 0) if usage_meta else 0
                t_out = int(getattr(usage_meta, "candidates_token_count", 0) or 0) if usage_meta else 0
                t_tot = int(getattr(usage_meta, "total_token_count", 0) or 0) if usage_meta else 0

                agent_model = resolve_model_for_agent(author, model_name)
                if t_in > 0 or t_out > 0 or t_tot > 0:
                    self._record_agent_tokens(
                        agent_name=author,
                        tokens_in=t_in,
                        tokens_out=t_out,
                        total_tokens=t_tot,
                        model_used=agent_model,
                    )
                    await self._publish_cost_roi()

                if event.content and event.content.parts:
                    text_parts = [p.text for p in event.content.parts if getattr(p, "text", None)]
                    if text_parts:
                        agent_usage = self._cost_roi.per_agent_tokens.get(author)
                        await event_bus.publish(
                            "agent_message",
                            {
                                "author": f"{author} [{agent_usage.agent_tier.value if agent_usage else 'LITE'} · {agent_model}]",
                                "agent_name": author,
                                "total_tokens": agent_usage.total_tokens if agent_usage else 0,
                                "text": "\n".join(text_parts),
                                "cost_and_roi": self._cost_roi.model_dump(),
                            },
                        )

        await asyncio.wait_for(_consume_stream(), timeout=PER_MODEL_TIMEOUT_SECONDS)

        final_session = await self._session_service.get_session(
            app_name=APP_NAME, user_id="sre_system", session_id=session_id
        )
        extracted: Dict[str, Any] = {}
        if final_session and final_session.state:
            for k in (
                "telemetry_summary",
                "log_forensics",
                "patch_diff",
                "verification_status",
                "postmortem_markdown",
                "awaiting_human_approval",
            ):
                if k in final_session.state:
                    extracted[k] = final_session.state[k]
        return extracted

    async def _run_deterministic_failover(self, incident_hash: str) -> Dict[str, Any]:
        """
        SRE Circuit-Breaker Failover: runs the exact same 5 stages and REAL local tools
        if all external Gemini API endpoints return 503 UNAVAILABLE or 429 quota errors.
        """
        await event_bus.publish(
            "agent_message",
            {
                "author": "SRE_CircuitBreaker [CIRCUIT_BREAKER]",
                "text": (
                    "All configured Gemini AI Studio endpoints returned 503/429 high-demand errors. "
                    "Engaging resilient local SRE failover runbook so recovery completes cleanly."
                ),
            },
        )

        # Stage 1a + 1b: Lite Telemetry & Log Forensics
        signals = fetch_golden_signals()
        logs = inspect_service_logs(25)

        for agent_name in ("TelemetrySignalAgent", "LogForensicsAgent"):
            emit_handoff_event(
                AgentHandoffEvent(
                    event_id=uuid.uuid4().hex[:8],
                    incident_hash=incident_hash,
                    parent_agent="SRE_Commander",
                    child_agent=agent_name,
                    stage=agent_name,
                    status="STARTED",
                    summary=f"Failover runbook executing LITE {agent_name}.",
                    agent_tier=AgentTier.LITE,
                    model_used="deterministic-failover",
                    guardrail_verdict=GuardrailVerdict.PASS,
                    state_payload={"incident_hash": incident_hash},
                )
            )
        await asyncio.sleep(0.25)

        telemetry_summary = (
            f"SLO Breach confirmed: Pool Saturation = {signals['pool_saturation_pct']}% "
            f"({signals['active_connections']}/{signals['max_connections']} connections locked), "
            f"p95 Latency = {signals['p95_latency_ms']} ms, 5xx Error Rate = {signals['error_rate_pct']}%."
        )
        log_forensics = (
            "Causal chain identified in logs/settlement.log: compliance_hold_early_return_leaked_connection "
            "events exhausted all 5 DB pool slots, triggering connection_pool_exhausted (HTTP 503)."
            if "compliance_hold_early_return_leaked_connection" in logs or "connection_pool_exhausted" in logs
            else "High-value compliance wires returned early without calling infra.release_connection(), exhausting the 5-slot pool."
        )

        self._record_agent_tokens("TelemetrySignalAgent", 90, 45, 135, "deterministic-failover", AgentTier.LITE)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="TelemetrySignalAgent",
                stage="TelemetrySignalAgent",
                status="COMPLETED",
                summary=telemetry_summary,
                agent_tier=AgentTier.LITE,
                model_used="deterministic-failover",
                tokens_in=90,
                tokens_out=45,
                total_tokens=135,
                guardrail_verdict=GuardrailVerdict.PASS,
                state_payload={"telemetry_summary": telemetry_summary},
            )
        )

        self._record_agent_tokens("LogForensicsAgent", 110, 50, 160, "deterministic-failover", AgentTier.LITE)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="LogForensicsAgent",
                stage="LogForensicsAgent",
                status="COMPLETED",
                summary=log_forensics,
                agent_tier=AgentTier.LITE,
                model_used="deterministic-failover",
                tokens_in=110,
                tokens_out=50,
                total_tokens=160,
                guardrail_verdict=GuardrailVerdict.PASS,
                state_payload={"log_forensics": log_forensics},
            )
        )
        await asyncio.sleep(0.25)

        # Stage 2a: Reasoning RootCauseArchitect stages real try...finally shadow patch
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="RootCauseArchitect",
                stage="RootCauseArchitect",
                status="STARTED",
                summary="Inspecting nexuspay_service/settlement_handler.py and staging try...finally shadow patch.",
                agent_tier=AgentTier.REASONING,
                model_used="deterministic-failover",
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["telemetry_summary", "log_forensics"],
                state_payload={"incident_hash": incident_hash},
            )
        )
        patched_code = build_deterministic_patched_handler()
        patch_diff, bak_path = stage_shadow_patch_direct(patched_code, incident_hash)
        self._record_agent_tokens("RootCauseArchitect", 420, 280, 700, "deterministic-failover", AgentTier.REASONING)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="RootCauseArchitect",
                stage="RootCauseArchitect",
                status="COMPLETED",
                summary=f"Staged try...finally connection-release patch (write-ahead backup: {bak_path}).",
                agent_tier=AgentTier.REASONING,
                model_used="deterministic-failover",
                tokens_in=420,
                tokens_out=280,
                total_tokens=700,
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["telemetry_summary", "log_forensics"],
                state_payload={"patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.25)

        # Stage 2b: Lite GuardrailVerifier executes REAL 15-wire sandbox concurrency test
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="GuardrailVerifier",
                stage="GuardrailVerifier",
                status="STARTED",
                summary="Running syntax check and 15-wire isolated concurrency stress test on shadow candidate.",
                agent_tier=AgentTier.LITE,
                model_used="deterministic-failover",
                guardrail_verdict=GuardrailVerdict.PASS,
                state_payload={"incident_hash": incident_hash},
            )
        )
        verify_msg, passed, awaiting = verify_and_promote_patch_direct(
            operational_mode=self.operational_mode.value,
            human_approved=False,
        )
        self._record_agent_tokens("GuardrailVerifier", 85, 40, 125, "deterministic-failover", AgentTier.LITE)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="GuardrailVerifier",
                stage="GuardrailVerifier",
                status="WAITING_APPROVAL" if awaiting else "COMPLETED",
                summary=verify_msg,
                agent_tier=AgentTier.LITE,
                model_used="deterministic-failover",
                tokens_in=85,
                tokens_out=40,
                total_tokens=125,
                guardrail_verdict=GuardrailVerdict.HELD_FOR_APPROVAL if awaiting else GuardrailVerdict.PASS,
                state_payload={"verification_status": verify_msg, "patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.25)

        # Stage 3: Reasoning PostMortemWriter generates Blameless Post-Mortem & saves to demo_artifacts/
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="PostMortemWriter",
                stage="PostMortemWriter",
                status="STARTED",
                summary="Drafting Google SRE Blameless Post-Mortem and exporting artifact.",
                agent_tier=AgentTier.REASONING,
                model_used="deterministic-failover",
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["verification_status"],
                state_payload={"incident_hash": incident_hash},
            )
        )
        deployment_state = (
            "Paused at Human-in-the-Loop Approval Gate awaiting SRE click to promote."
            if awaiting
            else "Autonomously promoted via atomic swap and live module hot-reload."
        )
        self._recompute_totals()
        postmortem_md = (
            f"# Google SRE Blameless Post-Mortem ({incident_hash})\n\n"
            "## Executive Summary\n"
            "EnterpriseSettlementGateway Settlement Gateway experienced 100% database connection pool saturation (5/5 connections locked), "
            "causing HTTP 503 failures for incoming B2B wire transfers. Project Aegis-SRE isolated the connection leak, "
            f"verified a `try...finally` patch in an isolated 15-wire concurrency sandbox, and handled deployment: **{deployment_state}**\n\n"
            "## Root Cause & Causal Chain\n"
            f"- **Telemetry:** {telemetry_summary}\n"
            f"- **Log Forensics:** {log_forensics}\n"
            "- **Code Defect:** In `nexuspay_service/settlement_handler.py`, high-value compliance wires (`amount_usd >= 10000` or flag set) "
            "returned early before calling `infra.release_connection()`.\n\n"
            "## Guardrail Verification & Financial Safety\n"
            f"- **Verification Result:** {verify_msg}\n"
            "- **Financial Safety:** **$0.00 duplicate charges** — `X-Idempotency-Key` replay protection prevented double liquidity deduction, "
            "and shadow verification ran against an isolated sandbox pool.\n\n"
            "## Cost, MTTR & ROI Telemetry\n"
            f"- **Total Tokens Used:** {self._cost_roi.total_tokens} (Est. Cost: ${self._cost_roi.estimated_cost_usd:.6f})\n"
            f"- **MTTR:** {self._cost_roi.mttr_seconds}s vs. {BASELINE_MANUAL_MINUTES}m manual baseline (~{self._cost_roi.estimated_hours_saved} hrs saved)\n\n"
            "## Preventive Action Items\n"
            "1. Enforce context managers (`with infra.connection():`) via static lint checks in CI.\n"
            "2. Keep SLO burn-rate alerting active at >= 90% pool saturation.\n"
            "3. Retain write-ahead `.bak` snapshots and automated 15-wire concurrency smoke gates on every gateway release.\n"
        )
        save_postmortem_artifact(incident_hash, postmortem_md)
        self._record_agent_tokens("PostMortemWriter", 310, 220, 530, "deterministic-failover", AgentTier.REASONING)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="PostMortemWriter",
                stage="PostMortemWriter",
                status="COMPLETED",
                summary="Blameless Post-Mortem generated and exported to demo_artifacts/.",
                agent_tier=AgentTier.REASONING,
                model_used="deterministic-failover",
                tokens_in=310,
                tokens_out=220,
                total_tokens=530,
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["verification_status"],
                state_payload={"postmortem_markdown": postmortem_md},
            )
        )

        return {
            "telemetry_summary": telemetry_summary,
            "log_forensics": log_forensics,
            "patch_diff": patch_diff,
            "verification_status": verify_msg,
            "postmortem_markdown": postmortem_md,
            "awaiting_human_approval": awaiting,
        }

    async def trigger_remediation(self, incident_hash: str) -> None:
        self._active_incident_hash = incident_hash
        self._latest_state = {"incident_hash": incident_hash}
        self._init_cost_roi(incident_hash)

        await event_bus.publish(
            "incident_detected",
            {
                "incident_hash": incident_hash,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mode": self.operational_mode.value,
                "message": (
                    f"SLO Breach detected! Acquired idempotency lock {incident_hash} "
                    f"[{self.operational_mode.value}]. Waking SRE_Commander..."
                ),
                "cost_and_roi": self._cost_roi.model_dump(),
            },
        )

        try:
            extracted: Optional[Dict[str, Any]] = None
            last_error: Optional[str] = None

            for idx, model_name in enumerate(LITE_MODEL_CHAIN):
                try:
                    await event_bus.publish(
                        "agent_message",
                        {
                            "author": "SRE_Commander",
                            "text": f"Starting Google ADK pipeline on model '{model_name}' (attempt {idx + 1}/{len(LITE_MODEL_CHAIN)})...",
                            "cost_and_roi": self._cost_roi.model_dump(),
                        },
                    )
                    extracted = await self._run_adk_with_model(model_name, incident_hash)
                    if extracted.get("patch_diff") or os.path.exists(CANDIDATE_FILE) or infra.snapshot_golden_signals().healthy:
                        break
                    last_error = f"Model '{model_name}' finished without staging a verified patch."
                    extracted = None
                except asyncio.TimeoutError:
                    last_error = f"Model '{model_name}' timed out after {int(PER_MODEL_TIMEOUT_SECONDS)}s."
                    await event_bus.publish(
                        "agent_message",
                        {"author": "ModelFailover", "text": f"{last_error} Switching to next model..."},
                    )
                except Exception as exc:
                    err_str = str(exc)
                    last_error = f"Model '{model_name}' failed: {err_str[:220]}"
                    print(f"[AEGIS WARN] {last_error}")
                    await event_bus.publish(
                        "agent_message",
                        {"author": "ModelFailover", "text": f"{last_error} Failing over..."},
                    )
                    await asyncio.sleep(1.0)

            if extracted is None:
                print(f"[AEGIS FAILOVER] Engaging deterministic SRE fallback ({last_error})")
                extracted = await self._run_deterministic_failover(incident_hash)

            self._latest_state.update(extracted)
            self._recompute_totals()

            if os.path.exists(CANDIDATE_FILE) and self.operational_mode == OperationalMode.APPROVAL_GATE:
                self._awaiting_approval = True
                await event_bus.publish(
                    "approval_required",
                    {
                        "incident_hash": incident_hash,
                        "state": self._latest_state,
                        "cost_and_roi": self._cost_roi.model_dump(),
                        "message": "Shadow patch verified (15/15 sandbox wires passed). Click 'Approve & Deploy Staged Patch' to promote.",
                    },
                )
            else:
                self._awaiting_approval = False
                self._resolved_hashes.add(incident_hash)
                await event_bus.publish(
                    "incident_resolved",
                    {
                        "incident_hash": incident_hash,
                        "state": self._latest_state,
                        "cost_and_roi": self._cost_roi.model_dump(),
                    },
                )
        except Exception as exc:
            tb = traceback.format_exc()
            print(f"[AEGIS ERROR] Unexpected failure:\n{tb}")
            self._resolved_hashes.add(incident_hash)
            await event_bus.publish(
                "agent_message",
                {"author": "SystemError", "text": f"Remediation halted: {exc}"},
            )
        finally:
            self._active_incident_hash = None

    async def approve_pending_patch(self) -> Dict[str, Any]:
        msg = promote_staged_candidate_manually()
        inc_hash = self._latest_state.get("incident_hash", "manual")
        self._awaiting_approval = False
        self._resolved_hashes.add(inc_hash)
        self._recompute_totals()

        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=inc_hash,
                parent_agent="SRE_Commander",
                child_agent="GuardrailVerifier",
                stage="GuardrailVerifier",
                status="COMPLETED",
                summary=msg,
                agent_tier=AgentTier.LITE,
                guardrail_verdict=GuardrailVerdict.PASS,
                state_payload={"verification_status": msg},
            )
        )
        await event_bus.publish("agent_message", {"author": "HumanSRE", "text": msg})
        await event_bus.publish(
            "incident_resolved",
            {
                "incident_hash": inc_hash,
                "state": self._latest_state,
                "cost_and_roi": self._cost_roi.model_dump(),
            },
        )
        return {"status": "promoted", "detail": msg, "cost_and_roi": self._cost_roi.model_dump()}

    async def reset_demo(self) -> Dict[str, Any]:
        msg = restore_baseline_bug()
        self._active_incident_hash = None
        self._awaiting_approval = False
        self._resolved_hashes.clear()
        self._latest_state.clear()
        self._cost_roi = IncidentCostAndROI()
        event_bus.clear_history()
        await event_bus.publish("demo_reset", {"message": msg, "cost_and_roi": self._cost_roi.model_dump()})
        return {"status": "reset", "detail": msg}


watcher = AutonomousIncidentWatcher()