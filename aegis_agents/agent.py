from __future__ import annotations
import os
from typing import Dict, List
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
from domain.schemas import AgentTier

load_dotenv()


def get_model_failover_chain(preferred_env: str | None = None) -> List[str]:
    """Ordered list of active Google AI Studio models to try before engaging fallback."""
    chain: List[str] = []
    if preferred_env and os.getenv(preferred_env):
        chain.append(os.environ[preferred_env].strip())
    for env_key in ("AEGIS_MODEL", "AEGIS_FAST_MODEL"):
        val = os.getenv(env_key)
        if val and val.strip() not in chain:
            chain.append(val.strip())
    for candidate in (
        "gemini-3.6-flash",
        "gemini-3.5-flash-lite",
        "gemini-3.8-flash",
        "gemini-3.1-flash-lite",
    ):
        if candidate not in chain:
            chain.append(candidate)
    return chain


LITE_MODEL_CHAIN = get_model_failover_chain("AEGIS_LITE_MODEL")
REASONING_MODEL_CHAIN = get_model_failover_chain("AEGIS_REASONING_MODEL")
MODEL_FAILOVER_CHAIN = LITE_MODEL_CHAIN

AGENT_TIER_MAP: Dict[str, AgentTier] = {
    "TelemetrySignalAgent": AgentTier.LITE,
    "LogForensicsAgent": AgentTier.LITE,
    "RootCauseArchitect": AgentTier.REASONING,
    "GuardrailVerifier": AgentTier.LITE,
    "PostMortemWriter": AgentTier.REASONING,
}


def get_agent_tier(agent_name: str) -> AgentTier:
    return AGENT_TIER_MAP.get(agent_name, AgentTier.LITE)


def resolve_model_for_agent(agent_name: str, fallback_model: str | None = None) -> str:
    """Routes Lite agents to a fast/compact model tier and Reasoning agents to the reasoning tier."""
    tier = get_agent_tier(agent_name)
    if tier == AgentTier.REASONING:
        if os.getenv("AEGIS_REASONING_MODEL"):
            return os.environ["AEGIS_REASONING_MODEL"].strip()
        return fallback_model or REASONING_MODEL_CHAIN[0]
    if os.getenv("AEGIS_LITE_MODEL"):
        return os.environ["AEGIS_LITE_MODEL"].strip()
    return fallback_model or LITE_MODEL_CHAIN[0]


def build_sre_commander(model_name: str) -> SequentialAgent:
    """
    Builds a Google ADK Sequential -> Parallel -> Loop multi-agent topology using
    Lite Agents for fast/low-token steps and Reasoning Agents for code patch & RCA synthesis.
    """
    lite_model = resolve_model_for_agent("TelemetrySignalAgent", model_name)
    reasoning_model = resolve_model_for_agent("RootCauseArchitect", model_name)

    # --- LITE STAGE 1A: GOLDEN SIGNALS SNAPSHOT (Compact prompt, minimal tokens) ---
    telemetry_signal_agent = Agent(
        name="TelemetrySignalAgent",
        model=lite_model,
        description="Lite agent: fetches and summarizes Golden Signals SLO breaches concisely.",
        instruction=(
            "Call fetch_golden_signals(). Output <=3 short bullets with breached signals, "
            "pool saturation %, and error rate."
        ),
        tools=[fetch_golden_signals],
        output_key="telemetry_summary",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    # --- LITE STAGE 1B: LOG FORENSICS EXTRACTOR (Compact prompt, minimal tokens) ---
    log_forensics_agent = Agent(
        name="LogForensicsAgent",
        model=lite_model,
        description="Lite agent: extracts root error event and failing code path from logs.",
        instruction=(
            "Call inspect_service_logs(). Output <=3 short bullets identifying the 503 trigger "
            "and leaked connection root cause."
        ),
        tools=[inspect_service_logs],
        output_key="log_forensics",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    triage_swarm = ParallelAgent(
        name="TriageSwarm",
        description="Runs Lite telemetry and log triage agents concurrently.",
        sub_agents=[telemetry_signal_agent, log_forensics_agent],
    )

    # --- REASONING STAGE 2A: ROOT CAUSE & SHADOW PATCH ARCHITECT ---
    root_cause_architect = Agent(
        name="RootCauseArchitect",
        model=reasoning_model,
        description="Reasoning agent: diagnoses code bug and stages a safe try...finally shadow patch.",
        instruction=(
            "You are a Principal SRE & Python Architect.\n"
            "Context: Telemetry={telemetry_summary?} | Logs={log_forensics?} | RetryFeedback={verification_status?}\n"
            "1. Call read_target_source_code() for 'nexuspay_service/settlement_handler.py'.\n"
            "2. Wrap connection usage in try...finally so infra.release_connection() always runs on early return.\n"
            "3. Keep function signatures, idempotency checks, and infra.deduct_liquidity(req.amount_usd) intact.\n"
            "4. Call stage_shadow_patch() with complete raw Python code (no markdown fences)."
        ),
        tools=[read_target_source_code, stage_shadow_patch],
        output_key="architect_notes",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    # --- LITE STAGE 2B: GUARDRAIL SANDBOX VERIFIER (Compact prompt, deterministic tool gate) ---
    guardrail_verifier = Agent(
        name="GuardrailVerifier",
        model=resolve_model_for_agent("GuardrailVerifier", lite_model),
        description="Lite gate agent: runs 15-wire sandbox verification and reports status concisely.",
        instruction=(
            "Call verify_and_promote_patch(). Return a concise 1-2 line verification summary."
        ),
        tools=[verify_and_promote_patch],
        output_key="verification_status",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    healing_loop = LoopAgent(
        name="GuardrailHealingLoop",
        description="Iteratively stages and verifies patches behind the sandbox guardrail.",
        sub_agents=[root_cause_architect, guardrail_verifier],
        max_iterations=2,
    )

    # --- REASONING STAGE 3: BLAMELESS POST-MORTEM WRITER ---
    postmortem_writer = Agent(
        name="PostMortemWriter",
        model=resolve_model_for_agent("PostMortemWriter", reasoning_model),
        description="Reasoning agent: writes executive blameless post-mortem and financial safety summary.",
        instruction=(
            "Write a concise SRE Blameless Post-Mortem in Markdown from:\n"
            "Incident={incident_hash?} | Telemetry={telemetry_summary?} | Logs={log_forensics?} | "
            "Patch={architect_notes?} | Guardrail={verification_status?}\n"
            "Sections required: ## Executive Summary, ## Root Cause & Causal Chain, "
            "## Guardrail Verification & Financial Safety ($0.00 duplicate charges via X-Idempotency-Key), "
            "## Preventive Action Items."
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


root_agent = build_sre_commander(LITE_MODEL_CHAIN[0])