from __future__ import annotations
import json
import os
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, Optional

PATTERN_STORE_PATH = os.path.join("demo_artifacts", "remediation_patterns.json")
os.makedirs("demo_artifacts", exist_ok=True)


def normalize_fault_signature(
    target_file: str,
    telemetry_summary: str = "",
    log_forensics: str = "",
    scenario_hint: Optional[str] = None,
) -> str:
    """Derives a canonical per-fault signature from scenario hint, Golden Signals, and logs."""
    if scenario_hint:
        return f"{scenario_hint}:{os.path.basename(target_file)}"

    combined = f"{telemetry_summary} {log_forensics}".lower()
    if "sanctions_review" in combined:
        fault_class = "connection_pool_leak_sanctions_variant"
    elif (
        "duplicate_charge" in combined
        or "without_idempotency_guard" in combined
        or ("idempotency" in combined and "missing" in combined)
    ):
        fault_class = "missing_idempotency_guard"
    elif "latency_slo_breach" in combined or "blocking_audit_call" in combined:
        fault_class = "latency_regression_spike"
    elif "upstream_fx_retry_storm" in combined or "retry_storm" in combined or "retry_multiplier" in combined:
        fault_class = "upstream_retry_storm"
    elif "compliance_hold" in combined or "pool_exhausted" in combined or "release_connection" in combined or "saturation" in combined:
        fault_class = "connection_pool_leak"
    else:
        tokens = re.findall(r"[a-z_]{5,}", combined)
        fault_class = "_".join(dict.fromkeys(tokens[:3])) or "generic_runtime_fault"

    return f"{fault_class}:{os.path.basename(target_file)}"


def _fault_class_similarity(cached_class: str, incoming_class: str) -> float:
    """Computes word-token Jaccard similarity so unrelated fault classes never cross-match."""
    if cached_class == incoming_class:
        return 1.0
    cached_tokens = {t for t in cached_class.split("_") if t}
    incoming_tokens = {t for t in incoming_class.split("_") if t}
    if not cached_tokens or not incoming_tokens:
        return 0.0
    union = cached_tokens | incoming_tokens
    return len(cached_tokens & incoming_tokens) / len(union)


def load_patterns() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(PATTERN_STORE_PATH):
        return {}
    try:
        with open(PATTERN_STORE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def clear_pattern_memory() -> str:
    """Clears cached remediation patterns so Cold (NOVEL_FAULT) runs can be demonstrated cleanly."""
    if os.path.exists(PATTERN_STORE_PATH):
        os.remove(PATTERN_STORE_PATH)
    return "Cleared Remediation Pattern Memory cache (remediation_patterns.json)."


def save_verified_pattern(
    fault_signature: str,
    target_file: str,
    patched_code: str,
    unified_diff: str,
    tokens_used: int = 0,
) -> Dict[str, Any]:
    """Persists a sandbox-verified fix pattern for future exact reuse or 60/40 delta scaffolding."""
    patterns = load_patterns()
    existing = patterns.get(fault_signature, {})
    pass_count = int(existing.get("pass_count", 0)) + 1

    entry = {
        "fault_signature": fault_signature,
        "target_file": target_file,
        "patched_code": patched_code,
        "unified_diff": unified_diff,
        "pass_count": pass_count,
        "last_tokens_used": tokens_used,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    patterns[fault_signature] = entry
    os.makedirs(os.path.dirname(PATTERN_STORE_PATH), exist_ok=True)
    with open(PATTERN_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(patterns, f, indent=2)
    return entry


def lookup_remediation_pattern(
    fault_signature: str,
    current_source_code: str,
    telemetry_summary: str = "",
    log_forensics: str = "",
) -> Dict[str, Any]:
    """
    Matches incoming incident against verified patterns:
      - EXACT_REUSE: exact fault-class + target-file match (with same-module code_score >= 0.35),
        OR combined score >= 0.90 -> applies verified patch directly (0 LLM tokens).
      - DELTA_SCAFFOLD (0.50 - 0.89): related fault variant -> 60% deterministic pipeline + 40% lean AI delta.
      - NOVEL_FAULT (< 0.50): unrelated fault class -> full 5-agent ADK synthesis.
    """
    patterns = load_patterns()
    if not patterns:
        return {
            "match_mode": "NOVEL_FAULT",
            "similarity_pct": 0.0,
            "matched_signature": None,
            "estimated_tokens_saved": 0,
            "pattern": None,
        }

    incoming_class, _, incoming_file = fault_signature.partition(":")
    best_entry: Optional[Dict[str, Any]] = None
    best_score = 0.0
    exact_class_hit = False

    for sig, entry in patterns.items():
        cached_class, _, cached_file = sig.partition(":")
        if cached_file and incoming_file and cached_file != incoming_file:
            continue
        class_score = _fault_class_similarity(cached_class, incoming_class)
        code_score = SequenceMatcher(None, current_source_code, entry.get("patched_code", "")).ratio()
        combined_score = round((0.75 * class_score) + (0.25 * code_score), 4)

        if cached_class == incoming_class and code_score >= 0.35:
            best_entry = entry
            best_score = max(combined_score, 0.95)
            exact_class_hit = True
            break

        if combined_score > best_score:
            best_score = combined_score
            best_entry = entry

    similarity_pct = round(best_score * 100.0, 1)
    if best_entry and (exact_class_hit or best_score >= 0.90):
        return {
            "match_mode": "EXACT_REUSE",
            "similarity_pct": similarity_pct,
            "matched_signature": best_entry["fault_signature"],
            "estimated_tokens_saved": 8000,
            "pattern": best_entry,
        }
    if best_entry and best_score >= 0.50:
        return {
            "match_mode": "DELTA_SCAFFOLD",
            "similarity_pct": similarity_pct,
            "matched_signature": best_entry["fault_signature"],
            "estimated_tokens_saved": 5000,
            "pattern": best_entry,
        }
    return {
        "match_mode": "NOVEL_FAULT",
        "similarity_pct": similarity_pct,
        "matched_signature": best_entry["fault_signature"] if best_entry else None,
        "estimated_tokens_saved": 0,
        "pattern": None,
    }