from __future__ import annotations
import asyncio
import os
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

load_dotenv()

import nexuspay_service.settlement_handler as settlement_handler
from aegis_agents.chaos_scenarios import list_chaos_scenarios
from control_plane.event_bus import event_bus
from control_plane.watcher import watcher
from domain.schemas import OperationalMode, SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import ConnectionPoolExhaustedError, infra

RECENT_LEDGER: deque[dict] = deque(maxlen=25)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await watcher.start()
    yield
    await watcher.stop()


app = FastAPI(title="Project Aegis-SRE | NexusPay Gateway", lifespan=lifespan)


class ModeUpdateRequest(BaseModel):
    mode: OperationalMode


class ScenarioInjectRequest(BaseModel):
    scenario_id: str


# --- CUSTOMER PORTAL ENDPOINTS (NexusPay) ---
@app.post("/api/settle", response_model=SettlementReceipt)
def create_settlement(
    req: SettlementRequest,
    x_idempotency_key: Optional[str] = Header(default=None),
):
    idem_key = x_idempotency_key or f"idem-{uuid.uuid4().hex[:10]}"
    try:
        receipt = settlement_handler.execute_settlement(req, idempotency_key=idem_key)
        RECENT_LEDGER.appendleft(receipt.model_dump())
        return receipt
    except ConnectionPoolExhaustedError as exc:
        infra.record_transaction(latency_ms=2500.0, success=False)
        failed_entry = {
            "transaction_id": f"ERR-{uuid.uuid4().hex[:6].upper()}",
            "idempotency_key": idem_key,
            "merchant_id": req.merchant_id,
            "amount_usd": req.amount_usd,
            "status": "FAILED",
            "latency_ms": 2500.0,
            "detail": str(exc),
        }
        RECENT_LEDGER.appendleft(failed_entry)
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/ledger")
def get_ledger():
    return {
        "available_liquidity_usd": infra.available_liquidity_usd,
        "transactions": list(RECENT_LEDGER),
    }


@app.get("/api/metrics")
def get_metrics():
    return infra.snapshot_golden_signals()


# --- ADMIN / SRE CONTROL PLANE ENDPOINTS ---
@app.get("/api/chaos/scenarios")
def get_chaos_scenarios():
    return {"scenarios": list_chaos_scenarios()}


@app.post("/api/chaos/inject")
async def inject_chaos(body: ScenarioInjectRequest):
    """Injects ONE isolated failure scenario on demand and triggers Aegis-SRE remediation."""
    RECENT_LEDGER.clear()
    try:
        return await watcher.inject_scenario(body.scenario_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/chaos/surge")
async def trigger_settlement_surge():
    """Legacy endpoint: simulates a 12-wire concurrent high-value B2B burst."""

    def _fire_burst():
        def _single_wire(i: int):
            req = SettlementRequest(
                merchant_id=f"GLOBAL-TREASURY-{100 + i}",
                recipient_iban="GB29NWBK60161331926819",
                amount_usd=25000.00 + (i * 500),
                simulate_validation_warning=True,
            )
            idem = f"surge-{uuid.uuid4().hex[:8]}"
            try:
                r = settlement_handler.execute_settlement(req, idempotency_key=idem)
                RECENT_LEDGER.appendleft(r.model_dump())
            except ConnectionPoolExhaustedError as exc:
                infra.record_transaction(latency_ms=2500.0, success=False)
                RECENT_LEDGER.appendleft(
                    {
                        "transaction_id": f"ERR-{uuid.uuid4().hex[:6].upper()}",
                        "idempotency_key": idem,
                        "merchant_id": req.merchant_id,
                        "amount_usd": req.amount_usd,
                        "status": "FAILED",
                        "latency_ms": 2500.0,
                        "detail": str(exc),
                    }
                )

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(_single_wire, range(12)))

    await asyncio.to_thread(_fire_burst)
    return {"status": "surge_dispatched", "metrics": infra.snapshot_golden_signals().model_dump()}


@app.get("/api/admin/status")
def get_admin_status():
    return {
        "metrics": infra.snapshot_golden_signals().model_dump(),
        "watcher": watcher.get_status(),
    }


@app.post("/api/admin/mode")
def update_operational_mode(body: ModeUpdateRequest):
    return watcher.set_mode(body.mode)


@app.post("/api/admin/approve")
async def approve_patch():
    return await watcher.approve_pending_patch()


@app.post("/api/admin/clear-patterns")
async def clear_patterns():
    return await watcher.clear_patterns()


@app.post("/api/admin/reset")
async def reset_demo():
    RECENT_LEDGER.clear()
    return await watcher.reset_demo()


@app.get("/api/events/stream")
async def stream_events():
    return StreamingResponse(event_bus.subscribe(), media_type="text/event-stream")


# --- DUAL-ROUTE FRONTEND ('/' Customer Portal & '/admin' SRE Console) ---
@app.get("/")
def serve_customer_portal():
    return FileResponse(os.path.join("frontend", "index.html"))


@app.get("/admin")
def serve_admin_console():
    return FileResponse(os.path.join("frontend", "index.html"))