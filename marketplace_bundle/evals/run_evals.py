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

from aegis_agents.tools import (
    build_deterministic_patched_handler,
    restore_baseline_bug,
    stage_shadow_patch_direct,
    verify_and_promote_patch_direct,
)
from domain.schemas import SettlementRequest
from nexuspay_service.pool_manager import INITIAL_LIQUIDITY_USD, infra
import nexuspay_service.settlement_handler as settlement_handler


def _evaluate_connection_pool_leak() -> Dict[str, Any]:
    start = time.perf_counter()
    restore_baseline_bug()

    settled_count = 0
    failed_503_count = 0

    for i in range(7):
        req = SettlementRequest(
            merchant_id=f"EVAL-LEAK-{i}",
            recipient_iban="ANON000000000000000000",
            amount_usd=12000.0,
            simulate_validation_warning=True,
        )
        try:
            settlement_handler.execute_settlement(req, idempotency_key=f"eval-leak-{i}")
            settled_count += 1
        except Exception:
            failed_503_count += 1

    breached_before = not infra.snapshot_golden_signals().healthy
    patched_code = build_deterministic_patched_handler()
    diff_text, _ = stage_shadow_patch_direct(patched_code, "eval-pool-leak")
    verify_msg, passed, _ = verify_and_promote_patch_direct(operational_mode="AUTONOMOUS", human_approved=False)

    # Verify post-fix recovery & idempotency safety
    post_req = SettlementRequest(
        merchant_id="EVAL-POST-FIX",
        recipient_iban="ANON000000000000000000",
        amount_usd=10000.0,
        simulate_validation_warning=True,
    )
    r1 = settlement_handler.execute_settlement(post_req, idempotency_key="eval-idem-key-1")
    liq_after_first = infra.available_liquidity_usd
    r2 = settlement_handler.execute_settlement(post_req, idempotency_key="eval-idem-key-1")
    liq_after_replay = infra.available_liquidity_usd

    idempotency_safe = (
        r2.status == "IDEMPOTENT_REPLAY"
        and abs(liq_after_replay - liq_after_first) < 1e-6
        and liq_after_first < INITIAL_LIQUIDITY_USD
    )
    recovered_after = infra.snapshot_golden_signals().healthy
    elapsed = round(time.perf_counter() - start, 3)

    ok = (
        settled_count == 5
        and failed_503_count == 2
        and breached_before
        and bool(diff_text.strip())
        and passed
        and idempotency_safe
        and recovered_after
    )

    return {
        "scenario": "connection_pool_leak",
        "passed": ok,
        "duration_seconds": elapsed,
        "checks": {
            "initial_5_settled_2_503": settled_count == 5 and failed_503_count == 2,
            "slo_breach_detected": breached_before,
            "shadow_diff_generated": bool(diff_text.strip()),
            "sandbox_15_wire_gate_passed": passed,
            "idempotency_zero_double_charge": idempotency_safe,
            "pool_recovered_healthy": recovered_after,
        },
        "verification_detail": verify_msg,
    }


def _evaluate_retry_storm() -> Dict[str, Any]:
    start = time.perf_counter()
    restore_baseline_bug()
    infra.trigger_retry_storm(incoming_rps=850, retry_multiplier=5)
    signals_during_storm = infra.snapshot_golden_signals()

    patched_code = build_deterministic_patched_handler()
    diff_text, _ = stage_shadow_patch_direct(patched_code, "eval-retry-storm")
    verify_msg, passed, _ = verify_and_promote_patch_direct(operational_mode="AUTONOMOUS", human_approved=False)

    recovered_signals = infra.snapshot_golden_signals()
    elapsed = round(time.perf_counter() - start, 3)

    ok = (
        not signals_during_storm.healthy
        and signals_during_storm.error_rate_pct > 5.0
        and bool(diff_text.strip())
        and passed
        and recovered_signals.healthy
        and recovered_signals.active_connections == 0
    )

    return {
        "scenario": "upstream_retry_storm",
        "passed": ok,
        "duration_seconds": elapsed,
        "checks": {
            "storm_slo_degradation_detected": (not signals_during_storm.healthy) and signals_during_storm.error_rate_pct > 5.0,
            "shadow_diff_generated": bool(diff_text.strip()),
            "sandbox_15_wire_gate_passed": passed,
            "post_recovery_pool_zero_leaks": recovered_signals.healthy and recovered_signals.active_connections == 0,
        },
        "verification_detail": verify_msg,
    }


def main() -> int:
    os.makedirs("demo_artifacts", exist_ok=True)
    results: List[Dict[str, Any]] = [
        _evaluate_connection_pool_leak(),
        _evaluate_retry_storm(),
    ]
    restore_baseline_bug()

    all_passed = all(r["passed"] for r in results)
    report = {
        "suite": "project-aegis-sre-mission-2026-evals",
        "overall_passed": all_passed,
        "total_scenarios": len(results),
        "passed_scenarios": sum(1 for r in results if r["passed"]),
        "results": results,
    }

    report_path = os.path.join("demo_artifacts", "eval_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))
    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())