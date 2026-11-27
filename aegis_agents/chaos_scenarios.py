from __future__ import annotations
import importlib
import os
from typing import Any, Dict, List

from domain.schemas import SettlementRequest
from nexuspay_service.pool_manager import infra

TARGET_FILE = os.path.join("nexuspay_service", "settlement_handler.py")
CANDIDATE_FILE = os.path.join("nexuspay_service", "settlement_handler.candidate.py")


CHAOS_SCENARIOS: Dict[str, Dict[str, Any]] = {
    "connection_pool_leak": {
        "id": "connection_pool_leak",
        "title": "1. Connection Pool Exhaustion (Compliance Early Return)",
        "fault_class": "connection_pool_leak",
        "slo_signal": "Pool Saturation 100% (5/5) + HTTP 503",
        "demo_role": "Cold NOVEL_FAULT (~7-8k tok) -> Repeat EXACT_REUSE (0 tok)",
    },
    "missing_idempotency_guard": {
        "id": "missing_idempotency_guard",
        "title": "2. Missing Idempotency Guard (Duplicate Wire Charge)",
        "fault_class": "missing_idempotency_guard",
        "slo_signal": "ERROR duplicate_charge_detected + Financial Audit Breach",
        "demo_role": "Proves distinct fault class (NOVEL_FAULT, zero cross-cache pollution)",
    },
    "latency_regression_spike": {
        "id": "latency_regression_spike",
        "title": "3. Latency Regression Spike (Blocking Sync Call)",
        "fault_class": "latency_regression_spike",
        "slo_signal": "p95 Latency > 350ms (breaches < 250ms SLO)",
        "demo_role": "Proves latency-only SLO remediation & sandbox p95 <= 250ms gate",
    },
    "upstream_retry_storm": {
        "id": "upstream_retry_storm",
        "title": "4. Upstream FX Retry Storm (Unbounded 5x Retries)",
        "fault_class": "upstream_retry_storm",
        "slo_signal": "5xx Error Rate > 50% + SLO Burn Rate > 14.4x",
        "demo_role": "Proves storm containment & server overload recovery",
    },
    "connection_pool_leak_sanctions_variant": {
        "id": "connection_pool_leak_sanctions_variant",
        "title": "5. Pool Leak Variant — Sanctions Branch (60/40 Delta Showcase)",
        "fault_class": "connection_pool_leak_sanctions_variant",
        "slo_signal": "Pool Saturation 100% (5/5) on Sanctions Hold Path",
        "demo_role": "Hits DELTA_SCAFFOLD (~60% pattern reuse + ~40% AI delta) after Scenario 1",
    },
}


def list_chaos_scenarios() -> List[Dict[str, Any]]:
    return list(CHAOS_SCENARIOS.values())


def _build_buggy_code(scenario_id: str) -> str:
    if scenario_id == "connection_pool_leak":
        return '''from __future__ import annotations
import time
import uuid
from domain.schemas import SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import infra


def execute_settlement(req: SettlementRequest, idempotency_key: str) -> SettlementReceipt:
    """Processes a B2B wire transfer through NexusPay Core."""
    existing = infra.get_idempotent_receipt(idempotency_key)
    if existing is not None:
        replay = existing.model_copy(update={"status": "IDEMPOTENT_REPLAY"})
        infra.log_event("INFO", "idempotent_replay_prevented", idempotency_key=idempotency_key)
        return replay

    start = time.perf_counter()
    conn_id = infra.acquire_connection()

    # BUG: High-value compliance hold returns early without releasing DB connection
    if req.simulate_validation_warning or req.amount_usd >= 10000.0:
        time.sleep(0.06)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.deduct_liquidity(req.amount_usd)
        infra.log_event(
            "ALERT",
            "compliance_hold_early_return_leaked_connection",
            conn_id=conn_id,
            merchant_id=req.merchant_id,
            amount_usd=req.amount_usd,
        )
        infra.record_transaction(latency_ms=latency_ms, success=True)
        receipt = SettlementReceipt(
            transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
            idempotency_key=idempotency_key,
            merchant_id=req.merchant_id,
            amount_usd=req.amount_usd,
            status="SETTLED",
            latency_ms=latency_ms,
            detail="Settled with compliance audit flag (connection leaked)",
        )
        infra.save_idempotent_receipt(idempotency_key, receipt)
        return receipt

    time.sleep(0.04)
    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    infra.deduct_liquidity(req.amount_usd)
    infra.record_transaction(latency_ms=latency_ms, success=True)
    infra.log_event("INFO", "settlement_completed", conn_id=conn_id, merchant_id=req.merchant_id, latency_ms=latency_ms)
    receipt = SettlementReceipt(
        transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
        idempotency_key=idempotency_key,
        merchant_id=req.merchant_id,
        amount_usd=req.amount_usd,
        status="SETTLED",
        latency_ms=latency_ms,
    )
    infra.save_idempotent_receipt(idempotency_key, receipt)
    infra.release_connection()
    return receipt
'''

    if scenario_id == "missing_idempotency_guard":
        return '''from __future__ import annotations
import time
import uuid
from domain.schemas import SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import infra


def execute_settlement(req: SettlementRequest, idempotency_key: str) -> SettlementReceipt:
    """Processes a B2B wire transfer through NexusPay Core."""
    # BUG: Missing infra.get_idempotent_receipt(idempotency_key) replay guard!
    # Replayed X-Idempotency-Key requests deduct liquidity a second time.
    start = time.perf_counter()
    conn_id = infra.acquire_connection()
    try:
        time.sleep(0.04)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.deduct_liquidity(req.amount_usd)
        infra.record_transaction(latency_ms=latency_ms, success=True)
        infra.log_event(
            "INFO",
            "settlement_completed_without_idempotency_guard",
            conn_id=conn_id,
            merchant_id=req.merchant_id,
            idempotency_key=idempotency_key,
        )
        receipt = SettlementReceipt(
            transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
            idempotency_key=idempotency_key,
            merchant_id=req.merchant_id,
            amount_usd=req.amount_usd,
            status="SETTLED",
            latency_ms=latency_ms,
        )
        infra.save_idempotent_receipt(idempotency_key, receipt)
        return receipt
    finally:
        infra.release_connection()
'''

    if scenario_id == "latency_regression_spike":
        return '''from __future__ import annotations
import time
import uuid
from domain.schemas import SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import infra


def execute_settlement(req: SettlementRequest, idempotency_key: str) -> SettlementReceipt:
    """Processes a B2B wire transfer through NexusPay Core."""
    existing = infra.get_idempotent_receipt(idempotency_key)
    if existing is not None:
        replay = existing.model_copy(update={"status": "IDEMPOTENT_REPLAY"})
        infra.log_event("INFO", "idempotent_replay_prevented", idempotency_key=idempotency_key)
        return replay

    start = time.perf_counter()
    conn_id = infra.acquire_connection()
    try:
        # BUG: Blocking synchronous audit call causes 380ms+ latency regression (> 250ms SLO)
        time.sleep(0.38)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.deduct_liquidity(req.amount_usd)
        infra.record_transaction(latency_ms=latency_ms, success=True)
        infra.log_event(
            "WARN",
            "latency_slo_breach_blocking_audit_call",
            conn_id=conn_id,
            merchant_id=req.merchant_id,
            latency_ms=latency_ms,
            error=f"Settlement p95 latency {latency_ms}ms exceeded 250ms SLO threshold",
        )
        receipt = SettlementReceipt(
            transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
            idempotency_key=idempotency_key,
            merchant_id=req.merchant_id,
            amount_usd=req.amount_usd,
            status="SETTLED",
            latency_ms=latency_ms,
        )
        infra.save_idempotent_receipt(idempotency_key, receipt)
        return receipt
    finally:
        infra.release_connection()
'''

    if scenario_id == "upstream_retry_storm":
        return '''from __future__ import annotations
import time
import uuid
from domain.schemas import SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import infra

# BUG: Unbounded synchronous FX retry amplification (no backoff / circuit breaker)
UPSTREAM_FX_RETRY_AMPLIFIER = 5


def execute_settlement(req: SettlementRequest, idempotency_key: str) -> SettlementReceipt:
    """Processes a B2B wire transfer through NexusPay Core."""
    existing = infra.get_idempotent_receipt(idempotency_key)
    if existing is not None:
        replay = existing.model_copy(update={"status": "IDEMPOTENT_REPLAY"})
        infra.log_event("INFO", "idempotent_replay_prevented", idempotency_key=idempotency_key)
        return replay

    start = time.perf_counter()
    conn_id = infra.acquire_connection()
    try:
        for _ in range(UPSTREAM_FX_RETRY_AMPLIFIER):
            time.sleep(0.01)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.deduct_liquidity(req.amount_usd)
        infra.record_transaction(latency_ms=latency_ms, success=True)
        infra.log_event(
            "ERROR",
            "upstream_fx_retry_storm",
            conn_id=conn_id,
            merchant_id=req.merchant_id,
            error="Unbounded 5x upstream FX retry amplification caused 5xx storm",
        )
        receipt = SettlementReceipt(
            transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
            idempotency_key=idempotency_key,
            merchant_id=req.merchant_id,
            amount_usd=req.amount_usd,
            status="SETTLED",
            latency_ms=latency_ms,
        )
        infra.save_idempotent_receipt(idempotency_key, receipt)
        return receipt
    finally:
        infra.release_connection()
'''

    if scenario_id == "connection_pool_leak_sanctions_variant":
        return '''from __future__ import annotations
import time
import uuid
from domain.schemas import SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import infra


def execute_settlement(req: SettlementRequest, idempotency_key: str) -> SettlementReceipt:
    """Processes a B2B wire transfer through NexusPay Core (Sanctions Review Path)."""
    existing = infra.get_idempotent_receipt(idempotency_key)
    if existing is not None:
        replay = existing.model_copy(update={"status": "IDEMPOTENT_REPLAY"})
        infra.log_event("INFO", "idempotent_replay_prevented", idempotency_key=idempotency_key)
        return replay

    start = time.perf_counter()
    conn_id = infra.acquire_connection()

    # BUG VARIANT: Sanctions IBAN review returns early without calling infra.release_connection()
    if req.recipient_iban.startswith("DE") or req.amount_usd >= 5000.0:
        time.sleep(0.05)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.deduct_liquidity(req.amount_usd)
        infra.log_event(
            "ALERT",
            "sanctions_review_early_return_leaked_connection",
            conn_id=conn_id,
            merchant_id=req.merchant_id,
            recipient_iban=req.recipient_iban,
            error="Sanctions review branch returned early without releasing DB connection",
        )
        infra.record_transaction(latency_ms=latency_ms, success=True)
        receipt = SettlementReceipt(
            transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
            idempotency_key=idempotency_key,
            merchant_id=req.merchant_id,
            amount_usd=req.amount_usd,
            status="SETTLED",
            latency_ms=latency_ms,
            detail="Settled via sanctions review path (connection leaked)",
        )
        infra.save_idempotent_receipt(idempotency_key, receipt)
        return receipt

    time.sleep(0.04)
    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    infra.deduct_liquidity(req.amount_usd)
    infra.record_transaction(latency_ms=latency_ms, success=True)
    receipt = SettlementReceipt(
        transaction_id=f"TXN-{uuid.uuid4().hex[:8].upper()}",
        idempotency_key=idempotency_key,
        merchant_id=req.merchant_id,
        amount_usd=req.amount_usd,
        status="SETTLED",
        latency_ms=latency_ms,
    )
    infra.save_idempotent_receipt(idempotency_key, receipt)
    infra.release_connection()
    return receipt
'''

    raise ValueError(f"Unknown chaos scenario: {scenario_id}")


def inject_chaos_scenario(scenario_id: str) -> Dict[str, Any]:
    """
    Injects EXACTLY ONE isolated failure scenario into settlement_handler.py,
    reloads the module cleanly, and triggers that scenario's matching SLO breach.
    """
    if scenario_id not in CHAOS_SCENARIOS:
        raise ValueError(f"Unknown scenario_id '{scenario_id}'. Valid: {list(CHAOS_SCENARIOS.keys())}")

    # Clean isolation before injecting a single scenario
    if os.path.exists(CANDIDATE_FILE):
        os.remove(CANDIDATE_FILE)
    infra.reset_state()

    buggy_code = _build_buggy_code(scenario_id)
    with open(TARGET_FILE, "w", encoding="utf-8") as f:
        f.write(buggy_code)

    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)

    settled = 0
    failed = 0

    if scenario_id == "connection_pool_leak":
        for i in range(7):
            req = SettlementRequest(
                merchant_id=f"CHAOS-POOL-{i}",
                recipient_iban="DE89370400440532013000",
                amount_usd=15000.0,
                simulate_validation_warning=True,
            )
            try:
                live_handler.execute_settlement(req, idempotency_key=f"chaos-pool-{i}")
                settled += 1
            except Exception:
                failed += 1

    elif scenario_id == "missing_idempotency_guard":
        req = SettlementRequest(
            merchant_id="CHAOS-IDEM-AUDIT",
            recipient_iban="DE89370400440532013000",
            amount_usd=7500.0,
            simulate_validation_warning=False,
        )
        live_handler.execute_settlement(req, idempotency_key="chaos-duplicate-key-001")
        liq_after_first = infra.available_liquidity_usd
        live_handler.execute_settlement(req, idempotency_key="chaos-duplicate-key-001")
        liq_after_second = infra.available_liquidity_usd
        settled = 2
        if liq_after_second < liq_after_first:
            infra.log_event(
                "ERROR",
                "duplicate_charge_detected",
                idempotency_key="chaos-duplicate-key-001",
                duplicate_amount_usd=round(liq_after_first - liq_after_second, 2),
                error="Missing X-Idempotency-Key guard caused double liquidity deduction",
            )
            for _ in range(3):
                infra.record_transaction(latency_ms=310.0, success=False)
            failed = 3

    elif scenario_id == "latency_regression_spike":
        for i in range(4):
            req = SettlementRequest(
                merchant_id=f"CHAOS-LAT-{i}",
                recipient_iban="GB29NWBK60161331926819",
                amount_usd=2500.0,
                simulate_validation_warning=False,
            )
            live_handler.execute_settlement(req, idempotency_key=f"chaos-lat-{i}")
            settled += 1

    elif scenario_id == "upstream_retry_storm":
        infra.trigger_retry_storm(incoming_rps=850, retry_multiplier=5)
        infra.log_event(
            "ERROR",
            "upstream_fx_retry_storm",
            incoming_rps=850,
            retry_multiplier=5,
            error="Unbounded 5x upstream FX retry amplification triggered HTTP 503 storm",
        )
        failed = 5

    elif scenario_id == "connection_pool_leak_sanctions_variant":
        for i in range(7):
            req = SettlementRequest(
                merchant_id=f"CHAOS-SANCTIONS-{i}",
                recipient_iban="DE89370400440532013000",
                amount_usd=6500.0,
                simulate_validation_warning=False,
            )
            try:
                live_handler.execute_settlement(req, idempotency_key=f"chaos-sanctions-{i}")
                settled += 1
            except Exception:
                failed += 1

    signals = infra.snapshot_golden_signals().model_dump()
    return {
        "scenario": CHAOS_SCENARIOS[scenario_id],
        "settled": settled,
        "failed": failed,
        "signals": signals,
    }