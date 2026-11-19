from __future__ import annotations
from google.adk.agents import Agent, LoopAgent, ParallelAgent, SequentialAgent
from aegis_agents.callbacks import on_child_agent_end, on_child_agent_start
from aegis_agents.tools import (
    fetch_golden_signals,
    inspect_service_logs,
    read_target_source_code,
    stage_shadow_patch,
    verify_and_promote_patch,
)

# --- STAGE 1: PARALLEL TRIAGE SWARM (Ultra-low latency via Gemini 2.5 Flash) ---
telemetry_signal_agent = Agent(
    name="TelemetrySignalAgent",
    model="gemini-2.5-flash",
    description="Analyzes Google SRE Four Golden Signals and error budget burn rate.",
    instruction=(
        "Call fetch_golden_signals() immediately. Summarize which of the Four Golden Signals "
        "(Latency, Traffic, Errors, Saturation) breached SLO thresholds and quantify the pool saturation."
    ),
    tools=[fetch_golden_signals],
    output_key="telemetry_summary",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

log_forensics_agent = Agent(
    name="LogForensicsAgent",
    model="gemini-2.5-flash",
    description="Extracts root error signatures and failing code paths from structured logs.",
    instruction=(
        "Call inspect_service_logs() immediately. Identify the exact sequence of events leading to "
        "503 errors (look for leaked connections or early-return warnings) and summarize concisely."
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
    model="gemini-2.5-pro",
    description="Diagnoses code-level bugs and stages a safe shadow patch.",
    instruction=(
        "You are a Principal Google SRE & Python Architect.\n"
        "Context from TriageSwarm:\n"
        "- Telemetry: {telemetry_summary}\n"
        "- Logs: {log_forensics}\n"
        "- Previous Verification Feedback (if any): {verification_status?}\n\n"
        "1. Call read_target_source_code() to inspect 'nexuspay_service/settlement_handler.py'.\n"
        "2. Fix the connection pool leak cleanly using a try...finally block so infra.release_connection() "
        "is ALWAYS called once conn_id is acquired, even when simulate_validation_warning or amount_usd >= 10000.0 triggers.\n"
        "3. Preserve all existing function signatures, imports, idempotency logic, and logging.\n"
        "4. Pass the complete, valid Python file content to stage_shadow_patch()."
    ),
    tools=[read_target_source_code, stage_shadow_patch],
    output_key="architect_notes",
    before_agent_callback=on_child_agent_start,
    after_agent_callback=on_child_agent_end,
)

guardrail_verifier = Agent(
    name="GuardrailVerifier",
    model="gemini-2.5-flash",
    description="Executes shadow syntax & concurrency stress tests before promoting code.",
    instruction=(
        "Call verify_and_promote_patch() immediately to test the staged candidate against 15 concurrent "
        "compliance wire transfers. Report the exact verification result."
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
    model="gemini-2.5-flash",
    description="Writes a Google SRE Blameless Post-Mortem and financial impact summary.",
    instruction=(
        "Write a concise, executive-ready Google SRE Blameless Post-Mortem in Markdown using:\n"
        "- Incident Hash: {incident_hash}\n"
        "- Telemetry Findings: {telemetry_summary}\n"
        "- Log Forensics: {log_forensics}\n"
        "- Architect Fix Notes: {architect_notes}\n"
        "- Guardrail Verification: {verification_status}\n\n"
        "Include sections: ## Executive Summary, ## Root Cause & Trigger, ## Guardrail Verification & Financial Safety "
        "(highlight $0.00 duplicate charges via X-Idempotency-Key), and ## Preventive Action Items."
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