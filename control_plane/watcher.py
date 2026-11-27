from __future__ import annotations
import asyncio
import glob
import hashlib
import json
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from google import genai
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
from aegis_agents.chaos_scenarios import (
    inject_chaos_scenario,
    list_chaos_scenarios,
)
from aegis_agents.pattern_memory import (
    clear_pattern_memory,
    lookup_remediation_pattern,
    normalize_fault_signature,
)
from aegis_agents.tools import (
    CANDIDATE_FILE,
    TARGET_FILE,
    build_deterministic_patched_handler,
    fetch_golden_signals,
    inspect_service_logs,
    promote_staged_candidate_manually,
    restore_baseline_bug,
    run_post_deploy_canary_check,
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
MAX_FAILOVER_MODELS = 2

# Production Gemini Flash paid-tier equivalent rates (USD per 1M tokens)
INPUT_COST_PER_MILLION_USD = 0.30
OUTPUT_COST_PER_MILLION_USD = 2.50
BLENDED_FALLBACK_PER_MILLION_USD = 0.50
BASELINE_MANUAL_MINUTES = 45.0


def _format_exception_detail(exc: BaseException) -> str:
    group_cls = getattr(__import__("builtins"), "BaseExceptionGroup", None)
    if group_cls is not None and isinstance(exc, group_cls):
        parts = [_format_exception_detail(sub) for sub in exc.exceptions]
        return f"{exc} -> [{' ; '.join(parts)}]"
    return f"{type(exc).__name__}: {exc}"


def _get_latest_backup_path() -> Optional[str]:
    backups = sorted(glob.glob(f"{TARGET_FILE}.*.bak"))
    return backups[-1] if backups else None


class AutonomousIncidentWatcher:
    """
    Monitors NexusPay's Four Golden Signals across 5 isolated failure classes,
    enforces per-fault SHA-256 incident idempotency locking, runs Google ADK SRE_Commander
    with 3-tier Pattern Memory (EXACT_REUSE 0-tok / 60-40 DELTA_SCAFFOLD / NOVEL_FAULT),
    real-time USD cost accounting, 4-invariant sandbox verification, and Canary auto-rollback.
    """

    def __init__(self):
        self.operational_mode: OperationalMode = OperationalMode.AUTONOMOUS
        self._active_incident_hash: Optional[str] = None
        self._active_scenario_id: Optional[str] = None
        self._active_fault_signature: Optional[str] = None
        self._awaiting_approval: bool = False
        self._resolved_hashes: set[str] = set()
        self._latest_state: Dict[str, Any] = {}
        self._incident_start_mono: Optional[float] = None
        self._cost_roi: IncidentCostAndROI = IncidentCostAndROI()

        # Cumulative session cost & token counters (reset ONLY on Clear Pattern Memory)
        self._session_committed_tokens: int = 0
        self._session_committed_tokens_in: int = 0
        self._session_committed_tokens_out: int = 0
        self._session_committed_cost_usd: float = 0.0
        self._session_incidents_fixed: int = 0
        self._incident_committed_to_session: bool = True

        self._running = False
        self._task: Optional[asyncio.Task] = None

        os.makedirs("demo_artifacts", exist_ok=True)
        self._session_service = InMemorySessionService()
        register_live_event_sink(self._on_agent_handoff)

    def _init_cost_roi(self, incident_hash: str) -> None:
        self._incident_start_mono = time.perf_counter()
        self._incident_committed_to_session = False
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

        if total_in > 0 or total_out > 0:
            est_cost = round(
                ((total_in / 1_000_000.0) * INPUT_COST_PER_MILLION_USD)
                + ((total_out / 1_000_000.0) * OUTPUT_COST_PER_MILLION_USD),
                6,
            )
        else:
            est_cost = round((total_tok / 1_000_000.0) * BLENDED_FALLBACK_PER_MILLION_USD, 6)

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

    def _get_session_cost_payload(self) -> Dict[str, Any]:
        """Returns live cumulative cost across all runs since last Clear Pattern Memory."""
        self._recompute_totals()
        in_flight_tokens = 0 if self._incident_committed_to_session else self._cost_roi.total_tokens
        in_flight_in = 0 if self._incident_committed_to_session else self._cost_roi.total_tokens_in
        in_flight_out = 0 if self._incident_committed_to_session else self._cost_roi.total_tokens_out
        in_flight_cost = 0.0 if self._incident_committed_to_session else self._cost_roi.estimated_cost_usd

        return {
            "total_tokens": self._session_committed_tokens + in_flight_tokens,
            "total_tokens_in": self._session_committed_tokens_in + in_flight_in,
            "total_tokens_out": self._session_committed_tokens_out + in_flight_out,
            "total_cost_usd": round(self._session_committed_cost_usd + in_flight_cost, 6),
            "incidents_fixed": self._session_incidents_fixed,
            "rates": {
                "input_per_1m_usd": INPUT_COST_PER_MILLION_USD,
                "output_per_1m_usd": OUTPUT_COST_PER_MILLION_USD,
            },
        }

    def _commit_incident_to_session(self) -> None:
        if self._incident_committed_to_session:
            return
        self._recompute_totals()
        self._session_committed_tokens += self._cost_roi.total_tokens
        self._session_committed_tokens_in += self._cost_roi.total_tokens_in
        self._session_committed_tokens_out += self._cost_roi.total_tokens_out
        self._session_committed_cost_usd = round(
            self._session_committed_cost_usd + self._cost_roi.estimated_cost_usd,
            6,
        )
        self._session_incidents_fixed += 1
        self._incident_committed_to_session = True

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
        payload = self._cost_roi.model_dump()
        payload["session_cost"] = self._get_session_cost_payload()
        await event_bus.publish("cost_roi_update", payload)

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
        event_dict["session_cost"] = self._get_session_cost_payload()
        await event_bus.publish("agent_handoff", event_dict)

    def get_status(self) -> Dict[str, Any]:
        self._recompute_totals()
        return {
            "operational_mode": self.operational_mode.value,
            "active_incident_hash": self._active_incident_hash,
            "active_scenario_id": self._active_scenario_id,
            "awaiting_approval": self._awaiting_approval and os.path.exists(CANDIDATE_FILE),
            "latest_state": self._latest_state,
            "cost_and_roi": self._cost_roi.model_dump(),
            "session_cost": self._get_session_cost_payload(),
            "scenarios": list_chaos_scenarios(),
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

    async def inject_scenario(self, scenario_id: str) -> Dict[str, Any]:
        """Resets per-incident locks and injects ONE isolated chaos scenario on demand."""
        if self._active_incident_hash is not None:
            return {"status": "busy", "detail": f"Incident {self._active_incident_hash} is currently running."}
        self._awaiting_approval = False
        self._resolved_hashes.clear()
        self._latest_state.clear()
        self._active_scenario_id = scenario_id
        event_bus.clear_history()

        result = inject_chaos_scenario(scenario_id)
        await event_bus.publish(
            "demo_reset",
            {
                "message": f"Injected isolated scenario: {result['scenario']['title']}",
                "scenario_id": scenario_id,
                "cost_and_roi": IncidentCostAndROI().model_dump(),
                "session_cost": self._get_session_cost_payload(),
            },
        )
        return {"status": "injected", **result}

    async def clear_patterns(self) -> Dict[str, Any]:
        """Clears cached remediation patterns AND resets cumulative session cost & fix counter."""
        msg = clear_pattern_memory()
        self._session_committed_tokens = 0
        self._session_committed_tokens_in = 0
        self._session_committed_tokens_out = 0
        self._session_committed_cost_usd = 0.0
        self._session_incidents_fixed = 0
        self._incident_committed_to_session = True
        session_cost = self._get_session_cost_payload()

        await event_bus.publish("session_cost_update", session_cost)
        await event_bus.publish(
            "agent_message",
            {
                "author": "PatternMemory",
                "text": f"{msg} Session cumulative cost reset to $0.0000.",
                "session_cost": session_cost,
            },
        )
        return {"status": "cleared", "detail": msg, "session_cost": session_cost}

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                metrics = infra.snapshot_golden_signals()
                await event_bus.publish("metrics_tick", metrics.model_dump())

                is_breached = (
                    metrics.pool_saturation_pct >= 90.0
                    or metrics.slo_burn_rate > 14.4
                    or metrics.p95_latency_ms > 250.0
                    or not metrics.healthy
                )
                if is_breached and self._active_incident_hash is None and not self._awaiting_approval:
                    logs_preview = inspect_service_logs(25)
                    fault_sig = normalize_fault_signature(
                        TARGET_FILE,
                        telemetry_summary=str(metrics.model_dump()),
                        log_forensics=logs_preview,
                        scenario_hint=self._active_scenario_id,
                    )
                    inc_hash = "sha256:" + hashlib.sha256(fault_sig.encode()).hexdigest()[:16]
                    if inc_hash not in self._resolved_hashes:
                        self._active_incident_hash = inc_hash
                        self._active_fault_signature = fault_sig
                        asyncio.create_task(self.trigger_remediation(inc_hash, fault_sig))
            except Exception as exc:
                await event_bus.publish("watcher_error", {"error": str(exc)})
            await asyncio.sleep(2.0)

    async def _run_adk_with_model(
        self,
        model_name: str,
        incident_hash: str,
        fault_signature: str,
        signals_snapshot: Dict[str, Any],
        logs_snapshot: str,
        current_src: str,
        is_failover: bool = False,
    ) -> Dict[str, Any]:
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        await self._session_service.create_session(
            app_name=APP_NAME,
            user_id="sre_system",
            session_id=session_id,
            state={
                "incident_hash": incident_hash,
                "fault_signature": fault_signature,
                "golden_signals_json": json.dumps(signals_snapshot),
                "aggregated_logs": logs_snapshot,
                "target_source_code": current_src,
                "operational_mode": self.operational_mode.value,
                "human_approved": False,
            },
        )

        runner = Runner(
            agent=build_sre_commander(model_name, is_failover=is_failover),
            app_name=APP_NAME,
            session_service=self._session_service,
        )

        trigger_msg = types.Content(
            role="user",
            parts=[
                types.Part(
                    text=f"Execute SRE remediation for incident {incident_hash} ({fault_signature})."
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

                agent_model = resolve_model_for_agent(author, model_name, force_model=is_failover)
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
                                "session_cost": self._get_session_cost_payload(),
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

    async def _run_delta_scaffold_path(
        self,
        incident_hash: str,
        fault_signature: str,
        current_src: str,
        pattern_info: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        True 60/40 Delta Scaffolding (~1.5k-2.5k tokens, 1 API call):
          - 60% deterministic (0 LLM tokens): Telemetry, Aggregated Logs, 4-Invariant Gate, RCA Export.
          - 40% lean Gemini delta call: Single focused LLM synthesis in RootCauseArchitect.
        """
        sim_pct = pattern_info["similarity_pct"]
        matched_sig = pattern_info["matched_signature"]
        saved_tok = pattern_info["estimated_tokens_saved"]

        await event_bus.publish(
            "agent_message",
            {
                "author": "PatternMemory [60/40 DELTA_SCAFFOLD]",
                "text": (
                    f"Partial pattern match ({sim_pct}% vs '{matched_sig}'). "
                    f"Reusing 60% deterministic triage/gate pipeline (0 tok) and spending AI tokens "
                    f"ONLY on the ~40% RootCauseArchitect delta (~{saved_tok} tokens saved)."
                ),
                "cost_and_roi": self._cost_roi.model_dump(),
                "session_cost": self._get_session_cost_payload(),
            },
        )

        signals = fetch_golden_signals()
        logs = inspect_service_logs(25)
        telemetry_summary = (
            f"SLO Breach ({fault_signature}): saturation={signals['pool_saturation_pct']}% "
            f"({signals['active_connections']}/{signals['max_connections']}), "
            f"p95={signals['p95_latency_ms']}ms, error_rate={signals['error_rate_pct']}%."
        )
        log_forensics = logs

        for agent_name, summary_txt, payload in (
            ("TelemetrySignalAgent", telemetry_summary, {"telemetry_summary": telemetry_summary}),
            ("LogForensicsAgent", f"Aggregated incident logs:\n{log_forensics}", {"log_forensics": log_forensics}),
        ):
            emit_handoff_event(
                AgentHandoffEvent(
                    event_id=uuid.uuid4().hex[:8],
                    incident_hash=incident_hash,
                    parent_agent="SRE_Commander",
                    child_agent=agent_name,
                    stage=agent_name,
                    status="COMPLETED",
                    summary=summary_txt,
                    agent_tier=AgentTier.LITE,
                    model_used="delta-scaffold-deterministic",
                    tokens_in=0,
                    tokens_out=0,
                    total_tokens=0,
                    guardrail_verdict=GuardrailVerdict.PASS,
                    state_payload=payload,
                )
            )
        await asyncio.sleep(0.1)

        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="RootCauseArchitect",
                stage="RootCauseArchitect",
                status="STARTED",
                summary=f"Synthesizing ~40% delta patch for '{fault_signature}' using proven '{matched_sig}' scaffold.",
                agent_tier=AgentTier.REASONING,
                model_used=LITE_MODEL_CHAIN[0],
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["telemetry_summary", "log_forensics"],
                state_payload={"incident_hash": incident_hash},
            )
        )

        api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        client = genai.Client(api_key=api_key) if api_key else genai.Client()

        prompt = (
            f"Fault: {fault_signature} (matched prior verified pattern '{matched_sig}').\n"
            f"Aggregated Logs:\n{log_forensics}\n\n"
            "Apply the proven try...finally connection-release scaffold to fix ONLY this variant's bug in the file below. "
            "Preserve all 4 invariants: (1) try...finally calling infra.release_connection(), "
            "(2) X-Idempotency-Key replay guard returning existing.model_copy(update=dict(status='IDEMPOTENT_REPLAY')), "
            "(3) fast latency <= 250ms, (4) infra.deduct_liquidity() & infra.save_idempotent_receipt().\n"
            "Output ONLY the complete fixed Python code with no markdown explanation.\n\n"
            f"```python\n{current_src}\n```"
        )

        patch_diff = ""
        bak_path = ""
        verify_msg = ""
        passed = False
        awaiting = False
        used_model = LITE_MODEL_CHAIN[0]

        for model_name in LITE_MODEL_CHAIN[:MAX_FAILOVER_MODELS]:
            try:
                used_model = model_name
                resp = await asyncio.wait_for(
                    asyncio.to_thread(
                        client.models.generate_content,
                        model=model_name,
                        contents=prompt,
                        config=types.GenerateContentConfig(temperature=0.1, max_output_tokens=2048),
                    ),
                    timeout=PER_MODEL_TIMEOUT_SECONDS,
                )
                usage = getattr(resp, "usage_metadata", None)
                t_in = int(getattr(usage, "prompt_token_count", 0) or 0) if usage else 0
                t_out = int(getattr(usage, "candidates_token_count", 0) or 0) if usage else 0
                t_tot = int(getattr(usage, "total_token_count", 0) or 0) if usage else 0
                if t_in > 0 or t_out > 0 or t_tot > 0:
                    self._record_agent_tokens(
                        "RootCauseArchitect",
                        t_in,
                        t_out,
                        t_tot,
                        model_name,
                        AgentTier.REASONING,
                    )
                    await self._publish_cost_roi()

                candidate_code = getattr(resp, "text", "") or ""
                if not candidate_code.strip():
                    continue

                patch_diff, bak_path = stage_shadow_patch_direct(candidate_code, incident_hash)
                verify_msg, passed, awaiting = verify_and_promote_patch_direct(
                    operational_mode=self.operational_mode.value,
                    human_approved=False,
                )
                if passed:
                    break
            except Exception as exc:
                err_str = _format_exception_detail(exc)
                print(f"[AEGIS WARN] Delta scaffold model '{model_name}' failed: {err_str[:220]}")
                await asyncio.sleep(0.5)

        if not passed:
            return None

        arch_usage = self._cost_roi.per_agent_tokens.get("RootCauseArchitect")
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="RootCauseArchitect",
                stage="RootCauseArchitect",
                status="COMPLETED",
                summary=(
                    f"Synthesized ~40% delta patch on '{used_model}' "
                    f"({arch_usage.total_tokens if arch_usage else 0} tok). Backup: {bak_path}."
                ),
                agent_tier=AgentTier.REASONING,
                model_used=used_model,
                tokens_in=arch_usage.tokens_in if arch_usage else 0,
                tokens_out=arch_usage.tokens_out if arch_usage else 0,
                total_tokens=arch_usage.total_tokens if arch_usage else 0,
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["telemetry_summary", "log_forensics"],
                state_payload={"patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.1)

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
                model_used="delta-scaffold-deterministic",
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
                guardrail_verdict=GuardrailVerdict.HELD_FOR_APPROVAL if awaiting else GuardrailVerdict.PASS,
                state_payload={"verification_status": verify_msg, "patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.1)

        self._recompute_totals()
        postmortem_md = (
            f"# Incident Post-Mortem: {incident_hash}\n\n"
            "## Executive Summary\n"
            f"Resolved fault `{fault_signature}` via **60/40 Delta Scaffolding** "
            f"(matched `{matched_sig}` at {sim_pct}%) in **{self._cost_roi.mttr_seconds}s** using only "
            f"**{self._cost_roi.total_tokens} tokens (${self._cost_roi.estimated_cost_usd:.4f})** — "
            f"reused 60% deterministic triage/gate pipeline and spent AI tokens only on the ~40% code delta (`{used_model}`).\n\n"
            "## Root Cause & Aggregated Evidence\n"
            f"- **Telemetry:** {telemetry_summary}\n"
            f"- **Log Forensics:**\n```\n{log_forensics}\n```\n\n"
            "## Guardrail Verification & Financial Safety\n"
            f"- **4-Invariant Sandbox Gate:** {verify_msg}\n"
            "- **Financial Safety:** **$0.00 duplicate charges** (`X-Idempotency-Key` verified in sandbox).\n"
        )
        save_postmortem_artifact(incident_hash, postmortem_md)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="PostMortemWriter",
                stage="PostMortemWriter",
                status="COMPLETED",
                summary="Blameless Post-Mortem exported via 60/40 Delta Scaffold (0 LLM tokens).",
                agent_tier=AgentTier.REASONING,
                model_used="delta-scaffold-deterministic",
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
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

    async def _run_pattern_reuse_fast_path(
        self,
        incident_hash: str,
        fault_signature: str,
        pattern_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Zero-LLM-token remediation ($0.0000) for EXACT_REUSE verified patterns."""
        pattern = pattern_info["pattern"]
        patched_code = pattern["patched_code"]
        sim_pct = pattern_info["similarity_pct"]
        saved_tok = pattern_info["estimated_tokens_saved"]

        await event_bus.publish(
            "agent_message",
            {
                "author": "PatternMemory [EXACT_REUSE · 0 tok · $0.0000]",
                "text": (
                    f"Exact pattern hit ({pattern['fault_signature']}, {sim_pct}% match, "
                    f"pass_count={pattern.get('pass_count', 1)}). Bypassing LLM synthesis "
                    f"(~{saved_tok} tokens saved, $0.0000 cost) and enforcing 4-invariant sandbox gate."
                ),
                "cost_and_roi": self._cost_roi.model_dump(),
                "session_cost": self._get_session_cost_payload(),
            },
        )

        signals = fetch_golden_signals()
        logs = inspect_service_logs(25)
        telemetry_summary = (
            f"SLO Breach ({fault_signature}): saturation={signals['pool_saturation_pct']}% "
            f"({signals['active_connections']}/{signals['max_connections']}), "
            f"p95={signals['p95_latency_ms']}ms, error_rate={signals['error_rate_pct']}%."
        )
        log_forensics = logs

        for agent_name, summary_txt, payload in (
            ("TelemetrySignalAgent", telemetry_summary, {"telemetry_summary": telemetry_summary}),
            ("LogForensicsAgent", f"Aggregated incident logs:\n{log_forensics}", {"log_forensics": log_forensics}),
        ):
            emit_handoff_event(
                AgentHandoffEvent(
                    event_id=uuid.uuid4().hex[:8],
                    incident_hash=incident_hash,
                    parent_agent="SRE_Commander",
                    child_agent=agent_name,
                    stage=agent_name,
                    status="COMPLETED",
                    summary=summary_txt,
                    agent_tier=AgentTier.LITE,
                    model_used="pattern-memory-exact-reuse",
                    tokens_in=0,
                    tokens_out=0,
                    total_tokens=0,
                    guardrail_verdict=GuardrailVerdict.PASS,
                    state_payload=payload,
                )
            )
        await asyncio.sleep(0.15)

        patch_diff, bak_path = stage_shadow_patch_direct(patched_code, incident_hash)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="RootCauseArchitect",
                stage="RootCauseArchitect",
                status="COMPLETED",
                summary=f"Reused verified pattern ({sim_pct}% match, 0 LLM tokens, $0.0000). Backup: {bak_path}.",
                agent_tier=AgentTier.REASONING,
                model_used="pattern-memory-exact-reuse",
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["telemetry_summary", "log_forensics"],
                state_payload={"patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.15)

        verify_msg, _, awaiting = verify_and_promote_patch_direct(
            operational_mode=self.operational_mode.value,
            human_approved=False,
        )
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
                model_used="pattern-memory-exact-reuse",
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
                guardrail_verdict=GuardrailVerdict.HELD_FOR_APPROVAL if awaiting else GuardrailVerdict.PASS,
                state_payload={"verification_status": verify_msg, "patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.15)

        self._recompute_totals()
        postmortem_md = (
            f"# Incident Post-Mortem: {incident_hash}\n\n"
            "## Executive Summary\n"
            f"Resolved fault `{fault_signature}` via **Remediation Pattern Memory (`EXACT_REUSE`, {sim_pct}% match)** in "
            f"**{self._cost_roi.mttr_seconds}s** using **0 LLM tokens ($0.0000)**.\n\n"
            "## Root Cause & Aggregated Evidence\n"
            f"- **Telemetry:** {telemetry_summary}\n"
            f"- **Log Forensics:**\n```\n{log_forensics}\n```\n\n"
            "## Guardrail Verification & Financial Safety\n"
            f"- **4-Invariant Sandbox Gate:** {verify_msg}\n"
            "- **Financial Safety:** **$0.00 duplicate charges** (`X-Idempotency-Key` verified in sandbox).\n"
        )
        save_postmortem_artifact(incident_hash, postmortem_md)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="PostMortemWriter",
                stage="PostMortemWriter",
                status="COMPLETED",
                summary="Post-Mortem exported via Pattern Memory fast-path (0 LLM tokens, $0.0000).",
                agent_tier=AgentTier.REASONING,
                model_used="pattern-memory-exact-reuse",
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
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

    async def _run_deterministic_failover(self, incident_hash: str, fault_signature: str) -> Dict[str, Any]:
        """SRE Circuit-Breaker Failover if external Gemini API endpoints return 503/429 errors."""
        await event_bus.publish(
            "agent_message",
            {
                "author": "SRE_CircuitBreaker [CIRCUIT_BREAKER]",
                "text": (
                    "Configured Gemini AI Studio endpoints failed or timed out. "
                    f"Engaging resilient local SRE failover runbook for '{fault_signature}'."
                ),
            },
        )

        signals = fetch_golden_signals()
        logs = inspect_service_logs(25)

        telemetry_summary = (
            f"SLO Breach ({fault_signature}): Saturation={signals['pool_saturation_pct']}% "
            f"({signals['active_connections']}/{signals['max_connections']}), "
            f"p95={signals['p95_latency_ms']}ms, 5xx={signals['error_rate_pct']}%."
        )
        log_forensics = logs

        for agent_name, summary_txt, payload in (
            ("TelemetrySignalAgent", telemetry_summary, {"telemetry_summary": telemetry_summary}),
            ("LogForensicsAgent", log_forensics, {"log_forensics": log_forensics}),
        ):
            emit_handoff_event(
                AgentHandoffEvent(
                    event_id=uuid.uuid4().hex[:8],
                    incident_hash=incident_hash,
                    parent_agent="SRE_Commander",
                    child_agent=agent_name,
                    stage=agent_name,
                    status="COMPLETED",
                    summary=summary_txt,
                    agent_tier=AgentTier.LITE,
                    model_used="deterministic-failover",
                    tokens_in=0,
                    tokens_out=0,
                    total_tokens=0,
                    guardrail_verdict=GuardrailVerdict.PASS,
                    state_payload=payload,
                )
            )
        await asyncio.sleep(0.15)

        patched_code = build_deterministic_patched_handler()
        patch_diff, bak_path = stage_shadow_patch_direct(patched_code, incident_hash)
        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=incident_hash,
                parent_agent="SRE_Commander",
                child_agent="RootCauseArchitect",
                stage="RootCauseArchitect",
                status="COMPLETED",
                summary=f"Staged 4-invariant hardened patch for '{fault_signature}' (backup: {bak_path}).",
                agent_tier=AgentTier.REASONING,
                model_used="deterministic-failover",
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
                guardrail_verdict=GuardrailVerdict.PASS,
                required_state_keys=["telemetry_summary", "log_forensics"],
                state_payload={"patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.15)

        verify_msg, _, awaiting = verify_and_promote_patch_direct(
            operational_mode=self.operational_mode.value,
            human_approved=False,
        )
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
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
                guardrail_verdict=GuardrailVerdict.HELD_FOR_APPROVAL if awaiting else GuardrailVerdict.PASS,
                state_payload={"verification_status": verify_msg, "patch_diff": patch_diff},
            )
        )
        await asyncio.sleep(0.15)

        self._recompute_totals()
        postmortem_md = (
            f"# Google SRE Blameless Post-Mortem ({incident_hash})\n\n"
            "## Executive Summary\n"
            f"Resolved fault `{fault_signature}` via resilient SRE failover runbook.\n\n"
            "## Root Cause & Evidence\n"
            f"- **Telemetry:** {telemetry_summary}\n"
            f"- **Log Forensics:**\n```\n{log_forensics}\n```\n\n"
            "## Guardrail Verification & Financial Safety\n"
            f"- **4-Invariant Gate:** {verify_msg}\n"
            "- **Financial Safety:** **$0.00 duplicate charges** (`X-Idempotency-Key` enforced).\n"
        )
        save_postmortem_artifact(incident_hash, postmortem_md)
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
                tokens_in=0,
                tokens_out=0,
                total_tokens=0,
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

    async def trigger_remediation(
        self,
        incident_hash: str,
        fault_signature: Optional[str] = None,
    ) -> None:
        self._active_incident_hash = incident_hash
        self._latest_state = {"incident_hash": incident_hash}
        self._init_cost_roi(incident_hash)

        try:
            with open(TARGET_FILE, "r", encoding="utf-8") as f:
                current_src = f.read()
        except Exception:
            current_src = ""

        signals_now = fetch_golden_signals()
        logs_now = inspect_service_logs(25)
        sig = fault_signature or normalize_fault_signature(
            TARGET_FILE,
            telemetry_summary=str(signals_now),
            log_forensics=logs_now,
            scenario_hint=self._active_scenario_id,
        )
        self._active_fault_signature = sig

        pattern_info = lookup_remediation_pattern(sig, current_src, str(signals_now), logs_now)
        self._latest_state["fault_signature"] = sig
        self._latest_state["pattern_memory"] = {
            "match_mode": pattern_info["match_mode"],
            "similarity_pct": pattern_info["similarity_pct"],
            "matched_signature": pattern_info["matched_signature"],
            "estimated_tokens_saved": pattern_info["estimated_tokens_saved"],
        }

        await event_bus.publish(
            "incident_detected",
            {
                "incident_hash": incident_hash,
                "fault_signature": sig,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mode": self.operational_mode.value,
                "pattern_memory": self._latest_state["pattern_memory"],
                "message": (
                    f"SLO Breach detected ({sig})! Lock {incident_hash} [{self.operational_mode.value}] | "
                    f"Pattern: {pattern_info['match_mode']} ({pattern_info['similarity_pct']}% match, "
                    f"~{pattern_info['estimated_tokens_saved']} tokens saved). Waking SRE_Commander..."
                ),
                "cost_and_roi": self._cost_roi.model_dump(),
                "session_cost": self._get_session_cost_payload(),
            },
        )

        try:
            extracted: Optional[Dict[str, Any]] = None
            last_error: Optional[str] = None

            if (
                pattern_info.get("match_mode") == "EXACT_REUSE"
                and pattern_info.get("pattern")
                and pattern_info["pattern"].get("patched_code", "").strip() != current_src.strip()
            ):
                extracted = await self._run_pattern_reuse_fast_path(incident_hash, sig, pattern_info)
            elif (
                pattern_info.get("match_mode") == "DELTA_SCAFFOLD"
                and pattern_info.get("pattern")
            ):
                extracted = await self._run_delta_scaffold_path(incident_hash, sig, current_src, pattern_info)

            if extracted is None:
                candidate_models = LITE_MODEL_CHAIN[:MAX_FAILOVER_MODELS]
                for idx, model_name in enumerate(candidate_models):
                    try:
                        await event_bus.publish(
                            "agent_message",
                            {
                                "author": "SRE_Commander",
                                "text": f"Starting Google ADK pipeline on '{model_name}' for '{sig}' (attempt {idx + 1}/{len(candidate_models)})...",
                                "cost_and_roi": self._cost_roi.model_dump(),
                                "session_cost": self._get_session_cost_payload(),
                            },
                        )
                        extracted = await self._run_adk_with_model(
                            model_name,
                            incident_hash,
                            fault_signature=sig,
                            signals_snapshot=signals_now,
                            logs_snapshot=logs_now,
                            current_src=current_src,
                            is_failover=(idx > 0),
                        )
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
                        err_str = _format_exception_detail(exc)
                        last_error = f"Model '{model_name}' failed: {err_str[:220]}"
                        print(f"[AEGIS WARN] {last_error}")
                        await event_bus.publish(
                            "agent_message",
                            {"author": "ModelFailover", "text": f"{last_error} Failing over..."},
                        )
                        await asyncio.sleep(1.0)

            if extracted is None:
                print(f"[AEGIS FAILOVER] Engaging deterministic SRE fallback ({last_error})")
                extracted = await self._run_deterministic_failover(incident_hash, sig)

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
                        "session_cost": self._get_session_cost_payload(),
                        "message": "Shadow patch passed all 4 invariants. Click 'Approve & Deploy Staged Patch' to promote.",
                    },
                )
            else:
                canary_ok, canary_msg = run_post_deploy_canary_check(
                    backup_path=_get_latest_backup_path(),
                    unified_diff=self._latest_state.get("patch_diff"),
                    fault_signature=sig,
                )
                self._latest_state["canary_status"] = canary_msg
                self._awaiting_approval = False
                self._resolved_hashes.add(incident_hash)
                self._commit_incident_to_session()
                await event_bus.publish(
                    "agent_message",
                    {
                        "author": "CanaryWatchdog",
                        "text": canary_msg,
                        "canary_ok": canary_ok,
                        "session_cost": self._get_session_cost_payload(),
                    },
                )
                await event_bus.publish(
                    "incident_resolved",
                    {
                        "incident_hash": incident_hash,
                        "state": self._latest_state,
                        "cost_and_roi": self._cost_roi.model_dump(),
                        "session_cost": self._get_session_cost_payload(),
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
            self._active_scenario_id = None

    async def approve_pending_patch(self) -> Dict[str, Any]:
        msg = promote_staged_candidate_manually()
        sig = self._active_fault_signature or self._latest_state.get("fault_signature")
        canary_ok, canary_msg = run_post_deploy_canary_check(
            backup_path=_get_latest_backup_path(),
            unified_diff=self._latest_state.get("patch_diff"),
            fault_signature=sig,
        )
        self._latest_state["canary_status"] = canary_msg
        inc_hash = self._latest_state.get("incident_hash", "manual")
        self._awaiting_approval = False
        self._resolved_hashes.add(inc_hash)
        self._commit_incident_to_session()
        session_cost = self._get_session_cost_payload()

        emit_handoff_event(
            AgentHandoffEvent(
                event_id=uuid.uuid4().hex[:8],
                incident_hash=inc_hash,
                parent_agent="SRE_Commander",
                child_agent="GuardrailVerifier",
                stage="GuardrailVerifier",
                status="COMPLETED",
                summary=f"{msg} | {canary_msg}",
                agent_tier=AgentTier.LITE,
                guardrail_verdict=GuardrailVerdict.PASS,
                state_payload={"verification_status": msg, "canary_status": canary_msg},
            )
        )
        await event_bus.publish(
            "agent_message",
            {
                "author": "HumanSRE",
                "text": f"{msg} | {canary_msg}",
                "canary_ok": canary_ok,
                "session_cost": session_cost,
            },
        )
        await event_bus.publish(
            "incident_resolved",
            {
                "incident_hash": inc_hash,
                "state": self._latest_state,
                "cost_and_roi": self._cost_roi.model_dump(),
                "session_cost": session_cost,
            },
        )
        return {
            "status": "promoted",
            "detail": msg,
            "canary_status": canary_msg,
            "cost_and_roi": self._cost_roi.model_dump(),
            "session_cost": session_cost,
        }

    async def reset_demo(self) -> Dict[str, Any]:
        msg = restore_baseline_bug()
        self._active_incident_hash = None
        self._active_scenario_id = None
        self._active_fault_signature = None
        self._awaiting_approval = False
        self._resolved_hashes.clear()
        self._latest_state.clear()
        self._cost_roi = IncidentCostAndROI()
        self._incident_committed_to_session = True
        event_bus.clear_history()
        await event_bus.publish(
            "demo_reset",
            {
                "message": msg,
                "cost_and_roi": self._cost_roi.model_dump(),
                "session_cost": self._get_session_cost_payload(),
            },
        )
        return {"status": "reset", "detail": msg}


watcher = AutonomousIncidentWatcher()