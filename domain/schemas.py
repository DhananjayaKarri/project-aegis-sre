from __future__ import annotations
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class OperationalMode(str, Enum):
    AUTONOMOUS = "AUTONOMOUS"
    APPROVAL_GATE = "APPROVAL_GATE"


class AgentTier(str, Enum):
    LITE = "LITE"
    REASONING = "REASONING"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"


class GuardrailVerdict(str, Enum):
    NOT_EVALUATED = "NOT_EVALUATED"
    PASS = "PASS"
    BLOCKED = "BLOCKED"
    HELD_FOR_APPROVAL = "HELD_FOR_APPROVAL"


class SettlementRequest(BaseModel):
    merchant_id: str = Field(..., min_length=2, max_length=64)
    recipient_iban: str = Field(..., min_length=5, max_length=64)
    amount_usd: float = Field(..., gt=0)
    simulate_validation_warning: bool = False


class SettlementReceipt(BaseModel):
    transaction_id: str
    idempotency_key: str
    merchant_id: str
    amount_usd: float
    status: str  # "SETTLED" | "IDEMPOTENT_REPLAY" | "FAILED"
    latency_ms: float
    detail: Optional[str] = None


class GoldenSignalsSnapshot(BaseModel):
    p95_latency_ms: float
    throughput_rps: float
    error_rate_pct: float
    pool_saturation_pct: float
    active_connections: int
    max_connections: int
    slo_burn_rate: float
    healthy: bool


class AgentTokenUsage(BaseModel):
    agent_name: str
    agent_tier: AgentTier = AgentTier.LITE
    model_used: str = "unknown"
    tokens_in: int = 0
    tokens_out: int = 0
    total_tokens: int = 0
    llm_calls: int = 0


class IncidentCostAndROI(BaseModel):
    incident_hash: str = ""
    total_tokens_in: int = 0
    total_tokens_out: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float = 0.0
    mttr_seconds: float = 0.0
    estimated_manual_minutes: float = 45.0
    estimated_hours_saved: float = 0.75
    cost_per_incident_usd: float = 0.0
    per_agent_tokens: Dict[str, AgentTokenUsage] = Field(default_factory=dict)


class AgentHandoffEvent(BaseModel):
    event_id: str
    incident_hash: str
    parent_agent: str
    child_agent: str
    stage: str
    status: str  # "STARTED" | "COMPLETED" | "WAITING_APPROVAL" | "BLOCKED"
    summary: str
    agent_tier: AgentTier = AgentTier.LITE
    model_used: str = "unknown"
    tokens_in: int = 0
    tokens_out: int = 0
    total_tokens: int = 0
    guardrail_verdict: GuardrailVerdict = GuardrailVerdict.NOT_EVALUATED
    guardrail_reason: Optional[str] = None
    required_state_keys: List[str] = Field(default_factory=list)
    state_payload: Dict[str, Any] = Field(default_factory=dict)