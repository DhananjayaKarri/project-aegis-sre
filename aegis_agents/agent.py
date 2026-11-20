from __future__ import annotations
import os
from typing import List
from dotenv import load_dotenv
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


def get_model_failover_chain() -> List[str]:
    """Ordered list of active Google AI Studio models to try before engaging fallback."""
    chain: List[str] = []
    custom = os.getenv("AEGIS_MODEL") or os.getenv("AEGIS_FAST_MODEL")
    if custom:
        chain.append(custom.strip())
    for candidate in (
        "gemini-3.6-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.8-flash",
        "gemini-3.1-flash-lite",
    ):
        if candidate not in chain:
            chain.append(candidate)
    return chain


MODEL_FAILOVER_CHAIN = get_model_failover_chain()


def build_sre_commander(model_name: str) -> SequentialAgent:
    """Builds a fresh Google ADK Sequential -> Parallel -> Loop multi-agent topology for a given model."""
    telemetry_signal_agent = Agent(
        name="TelemetrySignalAgent",
        model=model_name,
        description="Analyzes Google SRE Four Golden Signals and error budget burn rate.",
        instruction=(
            "Call fetch_golden_signals() immediately. In 3 concise bullet points, report which of the "
            "Four Golden Signals (Latency, Traffic, Errors, Saturation) breached SLO thresholds."
        ),
        tools=[fetch_golden_signals],
        output_key="telemetry_summary",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    log_forensics_agent = Agent(
        name="LogForensicsAgent",
        model=model_name,
        description="Extracts root error signatures and failing code paths from structured logs.",
        instruction=(
            "Call inspect_service_logs() immediately. In 3 concise bullet points, identify the exact causal chain "
            "leading to HTTP 503 connection_pool_exhausted errors (e.g., compliance_hold_early_return_leaked_connection)."
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

    root_cause_architect = Agent(
        name="RootCauseArchitect",
        model=model_name,
        description="Diagnoses code-level bugs and stages a safe shadow patch.",
        instruction=(
            "You are a Principal Google SRE & Python Architect.\n"
            "Context from TriageSwarm:\n"
            "- Telemetry: {telemetry_summary?}\n"
            "- Logs: {log_forensics?}\n"
            "- Previous Verification Feedback: {verification_status?}\n\n"
            "1. Call read_target_source_code() to inspect 'nexuspay_service/settlement_handler.py'.\n"
            "2. Fix the connection pool leak cleanly using a try...finally block so infra.release_connection() "
            "is ALWAYS executed after infra.acquire_connection(), even when simulate_validation_warning or amount_usd >= 10000.0 triggers.\n"
            "3. Preserve all imports, function signatures, idempotency replay checks, and infra.deduct_liquidity(req.amount_usd) calls.\n"
            "4. Call stage_shadow_patch() with the complete, raw Python file content (no markdown backticks)."
        ),
        tools=[read_target_source_code, stage_shadow_patch],
        output_key="architect_notes",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    guardrail_verifier = Agent(
        name="GuardrailVerifier",
        model=model_name,
        description="Executes shadow syntax & concurrency stress tests before promoting code.",
        instruction=(
            "Call verify_and_promote_patch() immediately to run syntax validation and the 15-wire isolated "
            "concurrency stress test. Report the exact verification result concisely."
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

    postmortem_writer = Agent(
        name="PostMortemWriter",
        model=model_name,
        description="Writes a Google SRE Blameless Post-Mortem and financial impact summary.",
        instruction=(
            "Write a concise, executive-ready Google SRE Blameless Post-Mortem in Markdown using:\n"
            "- Incident Hash: {incident_hash?}\n"
            "- Telemetry Findings: {telemetry_summary?}\n"
            "- Log Forensics: {log_forensics?}\n"
            "- Architect Fix Notes: {architect_notes?}\n"
            "- Guardrail Verification: {verification_status?}\n\n"
            "Include sections:\n"
            "## Executive Summary\n"
            "## Root Cause & Causal Chain\n"
            "## Guardrail Verification & Financial Safety ($0.00 duplicate charges via X-Idempotency-Key)\n"
            "## Preventive Action Items"
        ),
        output_key="postmortem_markdown",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    return SequentialAgent(
        name="SRE_Commander",
        description="Autonomous SRE Parent Orchestrator for Project Aegis-SRE (NexusPay).",
        sub_agents=[triage_swarm, healing_loop, postmortem_writer],
    )


# Default root_agent export for ADK CLI compatibility
root_agent = build_sre_commander(MODEL_FAILOVER_CHAIN[0])