from __future__ import annotations
from datetime import datetime, timezone
from enum import Enum
from typing import Literal
from pydantic import BaseModel, Field


class OperationalMode(str, Enum):
    AUTONOMOUS = "AUTONOMOUS"
    APPROVAL_GATE = "APPROVAL_GATE"


class SettlementRequest(BaseModel):
    merchant_id: str = Field(..., examples=["MERCH-8841"])
    recipient_iban: str = Field(..., examples=["DE89370400440532013000"])
    amount_usd: float = Field(..., gt=0, examples=[12500.00])
    currency: str = Field(default="USD")
    simulate_validation_warning: bool = Field(
        default=False,
        description="Triggers a minor validation path that exposes the connection leak.",
    )


class SettlementReceipt(BaseModel):
    transaction_id: str
    idempotency_key: str
    merchant_id: str
    amount_usd: float
    status: Literal["SETTLED", "FAILED", "IDEMPOTENT_REPLAY"]
    latency_ms: float
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    detail: str = "Settlement cleared via NexusPay Core"


class GoldenSignalsSnapshot(BaseModel):
    service_name: str = "nexuspay-settlement-gateway"
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    p95_latency_ms: float
    throughput_rps: float
    error_rate_pct: float
    pool_saturation_pct: float
    active_connections: int
    max_connections: int
    slo_burn_rate: float
    healthy: bool


class AgentHandoffEvent(BaseModel):
    event_id: str
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    incident_hash: str
    parent_agent: str
    child_agent: str
    stage: str
    status: Literal["STARTED", "COMPLETED", "FAILED", "WAITING_APPROVAL"]
    summary: str
    state_payload: dict = Field(default_factory=dict)