from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from aegis_agents.chaos_scenarios import inject_chaos_scenario
from aegis_agents.pattern_memory import (
    clear_pattern_memory,
    lookup_remediation_pattern,
    normalize_fault_signature,
)
from aegis_agents.tools import (
    TARGET_FILE,
    build_deterministic_patched_handler,
    restore_baseline_bug,
    run_post_deploy_canary_check,
    stage_shadow_patch_direct,
    verify_and_promote_patch_direct,
)
from nexuspay_service.pool_manager import infra


def _run_isolated_scenario_eval(
    scenario_id: str,
    expected_match_mode: str,
) -> Dict[str, Any]:
    start = time.perf_counter()
    injected = inject_chaos_scenario(scenario_id)
    signals_before = infra.snapshot_golden_signals()

    breach_detected = (
        not signals_before.healthy
        or signals_before.pool_saturation_pct >= 90.0
        or signals_before.p95_latency_ms > 250.0
        or signals_before.error_rate_pct > 5.0
    )

    with open(TARGET_FILE, "r", encoding="utf-8") as f:
        buggy_src = f.read()

    sig = normalize_fault_signature(TARGET_FILE, scenario_hint=scenario_id)
    pattern_hit = lookup_remediation_pattern(sig, buggy_src)

    patched_code = (
        pattern_hit["pattern"]["patched_code"]
        if pattern_hit["match_mode"] == "EXACT_REUSE" and pattern_hit.get("pattern")
        else build_deterministic_patched_handler()
    )
    diff_text, bak_path = stage_shadow_patch_direct(patched_code, f"eval-{scenario_id}")
    verify_msg, passed, _ = verify_and_promote_patch_direct(
        operational_mode="AUTONOMOUS",
        human_approved=False,
    )
    canary_ok, canary_msg = run_post_deploy_canary_check(
        backup_path=bak_path,
        unified_diff=diff_text,
        fault_signature=sig,
    )
    signals_after = infra.snapshot_golden_signals()
    elapsed = round(time.perf_counter() - start, 3)

    ok = (
        breach_detected
        and pattern_hit["match_mode"] == expected_match_mode
        and bool(diff_text.strip())
        and passed
        and canary_ok
        and signals_after.healthy
        and signals_after.active_connections == 0
    )

    return {
        "scenario": scenario_id,
        "expected_pattern_mode": expected_match_mode,
        "actual_pattern_mode": pattern_hit["match_mode"],
        "similarity_pct": pattern_hit["similarity_pct"],
        "estimated_tokens_saved": pattern_hit["estimated_tokens_saved"],
        "passed": ok,
        "duration_seconds": elapsed,
        "checks": {
            "breach_detected": breach_detected,
            "pattern_mode_matched": pattern_hit["match_mode"] == expected_match_mode,
            "shadow_diff_generated": bool(diff_text.strip()),
            "four_invariant_sandbox_passed": passed,
            "post_deploy_canary_healthy": canary_ok,
            "post_recovery_zero_leaks": signals_after.healthy and signals_after.active_connections == 0,
        },
        "injection_summary": {
            "settled": injected["settled"],
            "failed": injected["failed"],
        },
        "verification_detail": verify_msg,
        "canary_detail": canary_msg,
    }


def main() -> int:
    os.makedirs("demo_artifacts", exist_ok=True)
    clear_pattern_memory()

    # Ordered to prove: Cold NOVEL_FAULT -> Repeat EXACT_REUSE -> 60/40 DELTA_SCAFFOLD -> 3 distinct NOVEL_FAULTs
    plan = [
        ("connection_pool_leak", "NOVEL_FAULT"),
        ("connection_pool_leak", "EXACT_REUSE"),
        ("connection_pool_leak_sanctions_variant", "DELTA_SCAFFOLD"),
        ("missing_idempotency_guard", "NOVEL_FAULT"),
        ("latency_regression_spike", "NOVEL_FAULT"),
        ("upstream_retry_storm", "NOVEL_FAULT"),
    ]

    results: List[Dict[str, Any]] = [
        _run_isolated_scenario_eval(scenario_id, expected_mode)
        for scenario_id, expected_mode in plan
    ]

    restore_baseline_bug()
    clear_pattern_memory()

    all_passed = all(r["passed"] for r in results)
    report = {
        "suite": "project-aegis-sre-5-scenario-chaos-evals",
        "overall_passed": all_passed,
        "total_runs": len(results),
        "passed_runs": sum(1 for r in results if r["passed"]),
        "results": results,
    }

    report_path = os.path.join("demo_artifacts", "eval_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())