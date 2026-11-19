from __future__ import annotations
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

    # BUG: Missing try...finally block!
    # When a compliance/validation warning occurs (e.g., amount >= 10,000 or flag set),
    # the handler returns early without releasing conn_id back to the pool.
    if req.simulate_validation_warning or req.amount_usd >= 10000.0:
        time.sleep(0.06)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.log_event(
            "WARN",
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
            detail="Settled with compliance audit flag",
        )
        infra.save_idempotent_receipt(idempotency_key, receipt)
        return receipt

    time.sleep(0.04)
    infra.release_connection()
    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    infra.record_transaction(latency_ms=latency_ms, success=True)
    infra.log_event(
        "INFO",
        "settlement_completed",
        conn_id=conn_id,
        merchant_id=req.merchant_id,
        latency_ms=latency_ms,
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