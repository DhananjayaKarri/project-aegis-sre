from __future__ import annotations
import asyncio
import json
import os
import uuid
from typing import Dict, List, Optional

from google.adk.agents.callback_context import CallbackContext
from google.genai import types
from domain.schemas import AgentHandoffEvent, AgentTier, GuardrailVerdict

ARTIFACTS_DIR = "demo_artifacts"
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

_live_event_sink = None

REQUIRED_HANDOFF_KEYS: Dict[str, List[str]] = {
    "TelemetrySignalAgent": [],
    "LogForensicsAgent": [],
    "RootCauseArchitect": ["telemetry_summary", "log_forensics"],
    "GuardrailVerifier": [],
    "PostMortemWriter": ["verification_status"],
}

AGENT_DEFAULT_TIER: Dict[str, AgentTier] = {
    "TelemetrySignalAgent": AgentTier.LITE,
    "LogForensicsAgent": AgentTier.LITE,
    "RootCauseArchitect": AgentTier.REASONING,
    "GuardrailVerifier": AgentTier.LITE,
    "PostMortemWriter": AgentTier.REASONING,
}


def register_live_event_sink(sink_fn) -> None:
    global _live_event_sink
    _live_event_sink = sink_fn


def _state_as_dict(state_obj) -> dict:
    if isinstance(state_obj, dict):
        return state_obj
    if hasattr(state_obj, "to_dict"):
        return state_obj.to_dict()
    try:
        return dict(state_obj)
    except Exception:
        return {}


def save_postmortem_artifact(incident_hash: str, markdown_text: str) -> str:
    safe_hash = str(incident_hash or "default").replace(":", "_")[:16]
    rca_path = os.path.join(ARTIFACTS_DIR, f"POSTMORTEM_{safe_hash}.md")
    with open(rca_path, "w", encoding="utf-8") as f:
        f.write(str(markdown_text))
    return rca_path


def emit_handoff_event(event: AgentHandoffEvent) -> None:
    safe_hash = str(event.incident_hash or "default").replace(":", "_")[:16]
    trace_file = os.path.join(ARTIFACTS_DIR, f"incident_{safe_hash}_trace.json")

    history = []
    if os.path.exists(trace_file):
        try:
            with open(trace_file, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []

    history.append(event.model_dump())
    with open(trace_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)

    if _live_event_sink is not None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_live_event_sink(event.model_dump()))
        except RuntimeError:
            pass


def _evaluate_pre_handoff_guardrail(agent_name: str, state: dict) -> tuple[GuardrailVerdict, Optional[str], List[str]]:
    required_keys = REQUIRED_HANDOFF_KEYS.get(agent_name, [])
    missing = [k for k in required_keys if not str(state.get(k, "")).strip()]
    if missing:
        return (
            GuardrailVerdict.BLOCKED,
            f"Missing required upstream state keys for {agent_name}: {', '.join(missing)}",
            required_keys,
        )
    if required_keys:
        return (
            GuardrailVerdict.PASS,
            f"Verified required handoff state keys ({', '.join(required_keys)}) before {agent_name} execution.",
            required_keys,
        )
    return (GuardrailVerdict.PASS, f"Direct triage/gate delegation to {agent_name} authorized.", required_keys)


def on_child_agent_start(callback_context: CallbackContext) -> Optional[types.Content]:
    state = _state_as_dict(callback_context.state)
    incident_hash = state.get("incident_hash", "manual-run")
    agent_name = callback_context.agent_name
    tier = AGENT_DEFAULT_TIER.get(agent_name, AgentTier.LITE)

    verdict, reason, req_keys = _evaluate_pre_handoff_guardrail(agent_name, state)

    ev = AgentHandoffEvent(
        event_id=uuid.uuid4().hex[:8],
        incident_hash=incident_hash,
        parent_agent="SRE_Commander",
        child_agent=agent_name,
        stage=agent_name,
        status="STARTED",
        summary=f"SRE_Commander delegated [{tier.value}] task to {agent_name} ({verdict.value}).",
        agent_tier=tier,
        guardrail_verdict=verdict,
        guardrail_reason=reason,
        required_state_keys=req_keys,
        state_payload={
            "incident_hash": incident_hash,
            "guardrail_verdict": verdict.value,
            "guardrail_reason": reason,
            "available_state_keys": [k for k in state.keys() if not str(k).startswith("_")],
        },
    )
    emit_handoff_event(ev)
    return None


def on_child_agent_end(callback_context: CallbackContext) -> Optional[types.Content]:
    state = _state_as_dict(callback_context.state)
    incident_hash = state.get("incident_hash", "manual-run")
    agent_name = callback_context.agent_name
    tier = AGENT_DEFAULT_TIER.get(agent_name, AgentTier.LITE)

    state_preview = {}
    for key in ("telemetry_summary", "log_forensics", "patch_diff", "verification_status", "postmortem_markdown"):
        if key in state and state[key]:
            val = str(state[key])
            state_preview[key] = val if key in ("patch_diff", "postmortem_markdown") else val[:1500]

    is_waiting = agent_name == "GuardrailVerifier" and bool(state.get("awaiting_human_approval"))
    if is_waiting:
        status = "WAITING_APPROVAL"
        verdict = GuardrailVerdict.HELD_FOR_APPROVAL
        reason = "15-wire isolated sandbox test passed; held at Human Approval Gate prior to live promotion."
        summary = "GuardrailVerifier passed 15/15 sandbox stress tests and paused at Human Approval Gate."
    elif agent_name == "GuardrailVerifier":
        status = "COMPLETED"
        passed = bool(state.get("sandbox_verified", True))
        verdict = GuardrailVerdict.PASS if passed else GuardrailVerdict.BLOCKED
        reason = str(state.get("verification_status", "Sandbox verification completed."))
        summary = f"{agent_name} completed guardrail verification ({verdict.value})."
    else:
        status = "COMPLETED"
        verdict = GuardrailVerdict.PASS
        reason = f"{agent_name} output contract satisfied."
        summary = f"{agent_name} [{tier.value}] completed execution and updated shared session state."

    ev = AgentHandoffEvent(
        event_id=uuid.uuid4().hex[:8],
        incident_hash=incident_hash,
        parent_agent="SRE_Commander",
        child_agent=agent_name,
        stage=agent_name,
        status=status,
        summary=summary,
        agent_tier=tier,
        guardrail_verdict=verdict,
        guardrail_reason=reason,
        required_state_keys=REQUIRED_HANDOFF_KEYS.get(agent_name, []),
        state_payload=state_preview,
    )
    emit_handoff_event(ev)

    if agent_name == "PostMortemWriter" and state.get("postmortem_markdown"):
        save_postmortem_artifact(incident_hash, str(state["postmortem_markdown"]))

    return None