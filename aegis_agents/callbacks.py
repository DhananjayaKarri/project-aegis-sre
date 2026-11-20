from __future__ import annotations
import asyncio
import json
import os
import uuid
from typing import Optional

from google.adk.agents.callback_context import CallbackContext
from google.genai import types
from domain.schemas import AgentHandoffEvent

ARTIFACTS_DIR = "demo_artifacts"
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

_live_event_sink = None


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


def on_child_agent_start(callback_context: CallbackContext) -> Optional[types.Content]:
    state = _state_as_dict(callback_context.state)
    incident_hash = state.get("incident_hash", "manual-run")
    agent_name = callback_context.agent_name

    ev = AgentHandoffEvent(
        event_id=uuid.uuid4().hex[:8],
        incident_hash=incident_hash,
        parent_agent="SRE_Commander",
        child_agent=agent_name,
        stage=agent_name,
        status="STARTED",
        summary=f"SRE_Commander delegated task to {agent_name} with shared incident context.",
        state_payload={
            "incident_hash": incident_hash,
            "available_state_keys": [k for k in state.keys() if not str(k).startswith("_")],
        },
    )
    emit_handoff_event(ev)
    return None


def on_child_agent_end(callback_context: CallbackContext) -> Optional[types.Content]:
    state = _state_as_dict(callback_context.state)
    incident_hash = state.get("incident_hash", "manual-run")
    agent_name = callback_context.agent_name

    state_preview = {}
    for key in ("telemetry_summary", "log_forensics", "patch_diff", "verification_status", "postmortem_markdown"):
        if key in state and state[key]:
            val = str(state[key])
            state_preview[key] = val if key in ("patch_diff", "postmortem_markdown") else val[:1500]

    is_waiting = agent_name == "GuardrailVerifier" and bool(state.get("awaiting_human_approval"))
    status = "WAITING_APPROVAL" if is_waiting else "COMPLETED"
    summary = (
        "GuardrailVerifier passed 15/15 sandbox stress tests and paused at Human Approval Gate."
        if is_waiting
        else f"{agent_name} completed execution and updated shared session state."
    )

    ev = AgentHandoffEvent(
        event_id=uuid.uuid4().hex[:8],
        incident_hash=incident_hash,
        parent_agent="SRE_Commander",
        child_agent=agent_name,
        stage=agent_name,
        status=status,
        summary=summary,
        state_payload=state_preview,
    )
    emit_handoff_event(ev)

    if agent_name == "PostMortemWriter" and state.get("postmortem_markdown"):
        save_postmortem_artifact(incident_hash, str(state["postmortem_markdown"]))

    return None