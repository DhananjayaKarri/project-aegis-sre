from __future__ import annotations
import os
from typing import Dict, List
from dotenv import load_dotenv
from google.adk.agents import Agent, LoopAgent, ParallelAgent, SequentialAgent
from aegis_agents.callbacks import on_child_agent_end, on_child_agent_start
from aegis_agents.tools import (
    stage_shadow_patch,
    verify_and_promote_patch,
)
from domain.schemas import AgentTier

load_dotenv()

DEPRECATED_MODELS = {
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "models/gemini-2.5-flash",
    "models/gemini-2.5-flash-lite",
}


def _clean_env_model(env_key: str | None) -> str | None:
    if not env_key:
        return None
    val = (os.getenv(env_key) or "").strip()
    if not val or val in DEPRECATED_MODELS:
        return None
    return val


def get_model_failover_chain(preferred_env: str | None = None) -> List[str]:
    """Ordered list of active Google AI Studio models to try before engaging fallback."""
    chain: List[str] = []
    pref = _clean_env_model(preferred_env)
    if pref:
        chain.append(pref)
    for env_key in ("AEGIS_MODEL", "AEGIS_FAST_MODEL"):
        val = _clean_env_model(env_key)
        if val and val not in chain:
            chain.append(val)
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


def resolve_model_for_agent(
    agent_name: str,
    fallback_model: str | None = None,
    force_model: bool = False,
) -> str:
    """Routes Lite vs. Reasoning tiers while honoring explicit failover overrides."""
    if force_model and fallback_model and fallback_model not in DEPRECATED_MODELS:
        return fallback_model
    tier = get_agent_tier(agent_name)
    if tier == AgentTier.REASONING:
        env_reasoning = _clean_env_model("AEGIS_REASONING_MODEL")
        if env_reasoning:
            return env_reasoning
        return fallback_model or REASONING_MODEL_CHAIN[0]
    env_lite = _clean_env_model("AEGIS_LITE_MODEL")
    if env_lite:
        return env_lite
    return fallback_model or LITE_MODEL_CHAIN[0]


def build_sre_commander(model_name: str, is_failover: bool = False) -> SequentialAgent:
    """
    Builds a low-token Google ADK Sequential -> Parallel -> Loop multi-agent topology.
    Pre-fetched telemetry, aggregated logs, and target source are injected via session.state
    to eliminate 3 redundant tool-calling roundtrips per incident.
    """
    lite_model = resolve_model_for_agent("TelemetrySignalAgent", model_name, force_model=is_failover)
    reasoning_model = resolve_model_for_agent("RootCauseArchitect", model_name, force_model=is_failover)

    # --- LITE STAGE 1A: GOLDEN SIGNALS SNAPSHOT (1 API call, no redundant tool turn) ---
    telemetry_signal_agent = Agent(
        name="TelemetrySignalAgent",
        model=lite_model,
        include_contents="none",
        description="Lite agent: summarizes pre-fetched Golden Signals SLO breaches concisely.",
        instruction=(
            "Fault={fault_signature?} | Signals={golden_signals_json?}\n"
            "Output <=2 short bullets citing only breached Golden Signals (saturation %, p95 latency ms, error rate %). "
            "No root cause or fix suggestions."
        ),
        output_key="telemetry_summary",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    # --- LITE STAGE 1B: LOG FORENSICS EXTRACTOR (1 API call, fault-agnostic) ---
    log_forensics_agent = Agent(
        name="LogForensicsAgent",
        model=lite_model,
        include_contents="none",
        description="Lite agent: extracts root error event and failing code path from aggregated logs.",
        instruction=(
            "Fault={fault_signature?} | AggregatedLogs={aggregated_logs?}\n"
            "Output <=2 short bullets identifying the exact failing event and defect class from the logs "
            "(e.g., leaked connection on early return, missing idempotency guard, blocking latency sleep, or retry storm)."
        ),
        output_key="log_forensics",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    triage_swarm = ParallelAgent(
        name="TriageSwarm",
        description="Runs Lite telemetry and log triage agents concurrently.",
        sub_agents=[telemetry_signal_agent, log_forensics_agent],
    )

    # --- REASONING STAGE 2A: ROOT CAUSE & SHADOW PATCH ARCHITECT (Fault-Agnostic, 4 Invariants) ---
    root_cause_architect = Agent(
        name="RootCauseArchitect",
        model=reasoning_model,
        include_contents="none",
        description="Reasoning agent: fixes any NexusPay fault class and stages a 4-invariant shadow patch.",
        instruction=(
            "You are a Principal SRE & Python Architect fixing 'nexuspay_service/settlement_handler.py'.\n"
            "Fault={fault_signature?}\n"
            "Telemetry={telemetry_summary?}\n"
            "Logs={log_forensics?}\n"
            "RetryFeedback={verification_status?}\n"
            "CurrentSource:\n{target_source_code?}\n\n"
            "Fix the exact defect and enforce ALL 4 invariants on the FIRST attempt:\n"
            "1. Connection Safety: wrap DB usage in try...finally calling infra.release_connection() on all return paths.\n"
            "2. Idempotency Guard: BEFORE infra.acquire_connection(), check existing = infra.get_idempotent_receipt(idempotency_key); "
            "if existing is not None, return existing.model_copy(update=dict(status='IDEMPOTENT_REPLAY')).\n"
            "3. Latency & Storm Safety: keep execution fast (<= 0.06s sleep max; remove any blocking sleep > 0.1s or unbounded retry loops).\n"
            "4. Liquidity & Signatures: preserve execute_settlement(req, idempotency_key), infra.deduct_liquidity(req.amount_usd), "
            "infra.record_transaction(...), and infra.save_idempotent_receipt(idempotency_key, receipt).\n"
            "Call stage_shadow_patch() ONCE with complete raw Python code (no markdown fences), then reply with 1 short line."
        ),
        tools=[stage_shadow_patch],
        output_key="architect_notes",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    # --- LITE STAGE 2B: GUARDRAIL SANDBOX VERIFIER ---
    guardrail_verifier = Agent(
        name="GuardrailVerifier",
        model=resolve_model_for_agent("GuardrailVerifier", lite_model, force_model=is_failover),
        include_contents="none",
        description="Lite gate agent: runs 4-invariant sandbox verification and reports status concisely.",
        instruction=(
            "Call verify_and_promote_patch() once and return its exact 1-line outcome."
        ),
        tools=[verify_and_promote_patch],
        output_key="verification_status",
        before_agent_callback=on_child_agent_start,
        after_agent_callback=on_child_agent_end,
    )

    healing_loop = LoopAgent(
        name="GuardrailHealingLoop",
        description="Iteratively stages and verifies patches behind the 4-invariant sandbox guardrail.",
        sub_agents=[root_cause_architect, guardrail_verifier],
        max_iterations=2,
    )

    # --- REASONING STAGE 3: BLAMELESS POST-MORTEM WRITER ---
    postmortem_writer = Agent(
        name="PostMortemWriter",
        model=resolve_model_for_agent("PostMortemWriter", reasoning_model, force_model=is_failover),
        include_contents="none",
        description="Reasoning agent: writes executive blameless post-mortem and financial safety summary.",
        instruction=(
            "Write a concise SRE Blameless Post-Mortem in Markdown (under 180 words) from:\n"
            "Incident={incident_hash?} | Fault={fault_signature?} | Telemetry={telemetry_summary?} | "
            "Logs={log_forensics?} | Patch={architect_notes?} | Guardrail={verification_status?}\n"
            "Required sections: ## Executive Summary, ## Root Cause & Causal Chain, "
            "## Guardrail Verification & Financial Safety ($0.00 duplicate charges via X-Idempotency-Key), "
            "## Preventive Action Items.\n"
            "Use Guardrail status as source of truth; if approval is pending, state that deployment awaits human approval."
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