from __future__ import annotations
import os
from dotenv import load_dotenv
from google import genai
from google.adk.agents import Agent, LoopAgent, ParallelAgent, SequentialAgent
from aegis_agents.callbacks import on_child_agent_end, on_child_agent_start
from aegis_agents.tools import (
    fetch_golden_signals,
    inspect_service_logs,
    read_target_source_code,
    stage_shadow_patch,
    verify_and_promote_patch,
)

load_dotenv()


def _resolve_active_models() -> tuple[str, str]:
    """
    Auto-discovers valid Gemini Flash and Pro models enabled for the active GOOGLE_API_KEY
    so new AI Studio keys never fail with 404 model NOT_FOUND errors.
    """
    if os.getenv("AEGIS_FAST_MODEL") and os.getenv("AEGIS_PRO_MODEL"):
        return os.environ["AEGIS_FAST_MODEL"], os.environ["AEGIS_PRO_MODEL"]

    preferred_flash = [
        "gemini-3.8-flash",
        "gemini-3-flash-preview",
        "gemini-2.5-flash",
        "gemini-flash-latest",
    ]
    preferred_pro = [
        "gemini-3.8-pro",
        "gemini-3.1-pro-preview",
        "gemini-3-pro-preview",
        "gemini-2.5-pro",
        "gemini-pro-latest",
    ]

    try:
        client = genai.Client()
        available = set()
        for m in client.models.list():
            methods = getattr(m, "supported_actions", None) or getattr(m, "supported_generation_methods", None) or []
            if not methods or "generateContent" in methods:
                name = m.name.replace("models/", "")
                available.add(name)

        fast = next((m for m in preferred_flash if m in available), None)
        if not fast:
            fast = next((m for m in sorted(available, reverse=True) if "gemini" in m and "flash" in m and "image" not in m and "audio" not in m), "gemini-3.8-flash")

        pro = next((m for m in preferred_pro if m in available), None)
        if not pro:
            pro = next((m for m in sorted(available, reverse=True) if "gemini" in m and "pro" in m and "image" not in m), fast)

        print(f"[AEGIS-SRE] Resolved active Gemini models -> FAST={fast}, PRO={pro}")
        return fast, pro
    except Exception as exc:
        print(f"[AEGIS-SRE] Model discovery fallback ({exc}) -> using gemini-3.8-flash")
        return "gemini-3.8-flash", "gemini-3.8-flash"


FAST_MODEL, PRO_MODEL = _resolve_active_models()

# --- STAGE 1: PARALLEL TRIAGE SWARM ---
telemetry_signal_agent = Agent(
    name="TelemetrySignalAgent",
    model=FAST_MODEL,
    description="Analyzes Google SRE Four Golden Signals and error budget burn rate.",
    instruction=(
        "Call fetch_golden_signals() immediately. Report which of the Four Golden Signals "
        "(Latency, Traffic, Errors, Saturation) breached SLO thresholds and quantify the pool saturation."
    ),
    tools=[fetch_golden_signals],
    output_key="telemetry_summary",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

log_forensics_agent = Agent(
    name="LogForensicsAgent",
    model=FAST_MODEL,
    description="Extracts root error signatures and failing code paths from structured logs.",
    instruction=(
        "Call inspect_service_logs() immediately. Identify the exact causal chain leading to "
        "HTTP 503 connection_pool_exhausted errors (specifically compliance_hold_early_return_leaked_connection) "
        "and summarize the root trigger clearly."
    ),
    tools=[inspect_service_logs],
    output_key="log_forensics",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

triage_swarm = ParallelAgent(
    name="TriageSwarm",
    description="Runs Golden Signals telemetry analysis and log forensics concurrently.",
    sub_agents=[telemetry_signal_agent, log_forensics_agent],
)

# --- STAGE 2: SELF-CORRECTING PATCH & GUARDRAIL LOOP ---
root_cause_architect = Agent(
    name="RootCauseArchitect",
    model=PRO_MODEL,
    description="Diagnoses code-level bugs and stages a safe shadow patch.",
    instruction=(
        "You are Dr. AutoHeal / Principal Google SRE & Python Architect.\n"
        "Context from TriageSwarm:\n"
        "- Telemetry: {telemetry_summary}\n"
        "- Logs: {log_forensics}\n"
        "- Previous Verification Feedback (if any): {verification_status?}\n\n"
        "1. Call read_target_source_code() to inspect 'nexuspay_service/settlement_handler.py'.\n"
        "2. Fix the connection pool leak cleanly using a try...finally block so infra.release_connection() "
        "is ALWAYS called once conn_id is acquired, even when simulate_validation_warning or amount_usd >= 10000.0 triggers.\n"
        "3. Preserve all existing function signatures, imports, idempotency logic, infra.deduct_liquidity(), and logging.\n"
        "4. Pass the complete, valid Python file content to stage_shadow_patch()."
    ),
    tools=[read_target_source_code, stage_shadow_patch],
    output_key="architect_notes",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

guardrail_verifier = Agent(
    name="GuardrailVerifier",
    model=FAST_MODEL,
    description="Executes shadow syntax & concurrency stress tests before promoting code.",
    instruction=(
        "Call verify_and_promote_patch() immediately to test the staged candidate against 15 concurrent "
        "compliance wire transfers. Report the exact verification outcome."
    ),
    tools=[verify_and_promote_patch],
    output_key="verification_status",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

healing_loop = LoopAgent(
    name="GuardrailHealingLoop",
    description="Iteratively generates and verifies code patches behind an automated stress-test gate.",
    sub_agents=[root_cause_architect, guardrail_verifier],
    max_iterations=2,
)

# --- STAGE 3: GOOGLE-STYLE BLAMELESS POST-MORTEM WRITER ---
postmortem_writer = Agent(
    name="PostMortemWriter",
    model=FAST_MODEL,
    description="Writes a Google SRE Blameless Post-Mortem and financial impact summary.",
    instruction=(
        "Write a crisp, executive-ready Google SRE Blameless Post-Mortem in Markdown using:\n"
        "- Incident Hash: {incident_hash}\n"
        "- Telemetry Findings: {telemetry_summary}\n"
        "- Log Forensics: {log_forensics}\n"
        "- Architect Fix Notes: {architect_notes}\n"
        "- Guardrail Verification: {verification_status}\n\n"
        "Include sections:\n"
        "## Executive Summary\n"
        "## Root Cause & Causal Chain\n"
        "## Safe Recovery & Guardrail Verification (highlight $0.00 duplicate charges via X-Idempotency-Key)\n"
        "## Preventive Action Items"
    ),
    output_key="postmortem_markdown",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

# --- ROOT ORCHESTRATOR ---
root_agent = SequentialAgent(
    name="SRE_Commander",
    description="Autonomous SRE Parent Orchestrator for Project Aegis-SRE (NexusPay).",
    sub_agents=[triage_swarm, healing_loop, postmortem_writer],
)