from __future__ import annotations
import asyncio
import hashlib
import os
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

from aegis_agents.agent import root_agent
from aegis_agents.callbacks import register_live_event_sink
from aegis_agents.tools import (
    CANDIDATE_FILE,
    promote_staged_candidate_manually,
    restore_baseline_bug,
)
from control_plane.event_bus import event_bus
from domain.schemas import OperationalMode
from nexuspay_service.pool_manager import infra

APP_NAME = "project_aegis_sre"


class AutonomousIncidentWatcher:
    """
    Monitors NexusPay's Four Golden Signals, enforces SHA-256 incident idempotency locking,
    and triggers the Google ADK SRE_Commander multi-agent pipeline on SLO breach.
    """

    def __init__(self):
        self.operational_mode: OperationalMode = OperationalMode.AUTONOMOUS
        self._active_incident_hash: Optional[str] = None
        self._resolved_hashes: set[str] = set()
        self._latest_state: Dict[str, Any] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

        os.makedirs("demo_artifacts", exist_ok=True)
        self._session_service = InMemorySessionService()
        self._runner = Runner(
            agent=root_agent,
            app_name=APP_NAME,
            session_service=self._session_service,
        )

        # Connect ADK agent callbacks directly to the SSE event bus
        register_live_event_sink(self._on_agent_handoff)

    async def _on_agent_handoff(self, event_dict: Dict[str, Any]) -> None:
        state_payload = event_dict.get("state_payload", {})
        if state_payload:
            self._latest_state.update(state_payload)
        await event_bus.publish("agent_handoff", event_dict)

    def get_status(self) -> Dict[str, Any]:
        return {
            "operational_mode": self.operational_mode.value,
            "active_incident_hash": self._active_incident_hash,
            "awaiting_approval": os.path.exists(CANDIDATE_FILE)
            and self.operational_mode == OperationalMode.APPROVAL_GATE,
            "latest_state": self._latest_state,
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
                if is_breached and self._active_incident_hash is None:
                    sig = f"nexuspay-pool-saturation-{metrics.max_connections}"
                    inc_hash = "sha256:" + hashlib.sha256(sig.encode()).hexdigest()[:16]
                    if inc_hash not in self._resolved_hashes:
                        # Set lock synchronously BEFORE scheduling task so _poll_loop never double-fires!
                        self._active_incident_hash = inc_hash
                        asyncio.create_task(self.trigger_remediation(inc_hash))
            except Exception as exc:
                await event_bus.publish("watcher_error", {"error": str(exc)})
            await asyncio.sleep(2.0)

    async def trigger_remediation(self, incident_hash: str) -> None:
        self._active_incident_hash = incident_hash
        self._latest_state = {"incident_hash": incident_hash}

        await event_bus.publish(
            "incident_detected",
            {
                "incident_hash": incident_hash,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "mode": self.operational_mode.value,
                "message": f"SLO Breach detected! Acquired idempotency lock {incident_hash}. Waking SRE_Commander...",
            },
        )

        try:
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

            trigger_msg = types.Content(
                role="user",
                parts=[
                    types.Part(
                        text=(
                            f"ALERT: NexusPay Settlement Gateway breached SLO (Incident {incident_hash}). "
                            "Execute full parallel triage, stage & verify a code fix behind guardrails, "
                            "and produce the blameless post-mortem."
                        )
                    )
                ],
            )

            async for event in self._runner.run_async(
                user_id="sre_system",
                session_id=session_id,
                new_message=trigger_msg,
            ):
                if event.content and event.content.parts:
                    text_parts = [p.text for p in event.content.parts if getattr(p, "text", None)]
                    if text_parts:
                        await event_bus.publish(
                            "agent_message",
                            {
                                "author": event.author,
                                "text": "\n".join(text_parts),
                            },
                        )

            final_session = await self._session_service.get_session(
                app_name=APP_NAME, user_id="sre_system", session_id=session_id
            )
            if final_session and final_session.state:
                for k in ("telemetry_summary", "log_forensics", "patch_diff", "verification_status", "postmortem_markdown"):
                    if k in final_session.state:
                        self._latest_state[k] = final_session.state[k]

            if not (os.path.exists(CANDIDATE_FILE) and self.operational_mode == OperationalMode.APPROVAL_GATE):
                self._resolved_hashes.add(incident_hash)
                await event_bus.publish(
                    "incident_resolved",
                    {
                        "incident_hash": incident_hash,
                        "state": self._latest_state,
                    },
                )
        except Exception as exc:
            tb = traceback.format_exc()
            print(f"[AEGIS ERROR] Remediation pipeline failed:\n{tb}")
            await event_bus.publish(
                "agent_message",
                {
                    "author": "SystemError",
                    "text": f"Remediation error: {exc}\n{tb[-500:]}",
                },
            )
        finally:
            self._active_incident_hash = None

    async def approve_pending_patch(self) -> Dict[str, Any]:
        msg = promote_staged_candidate_manually()
        if self._latest_state.get("incident_hash"):
            self._resolved_hashes.add(self._latest_state["incident_hash"])
        await event_bus.publish(
            "incident_resolved",
            {
                "incident_hash": self._latest_state.get("incident_hash", "manual"),
                "state": self._latest_state,
            },
        )
        await event_bus.publish("agent_message", {"author": "HumanSRE", "text": msg})
        return {"status": "promoted", "detail": msg}

    async def reset_demo(self) -> Dict[str, Any]:
        msg = restore_baseline_bug()
        self._active_incident_hash = None
        self._resolved_hashes.clear()
        self._latest_state.clear()
        event_bus.clear_history()
        await event_bus.publish("demo_reset", {"message": msg})
        return {"status": "reset", "detail": msg}


watcher = AutonomousIncidentWatcher()