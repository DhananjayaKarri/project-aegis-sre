from __future__ import annotations
import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Optional
from domain.schemas import GoldenSignalsSnapshot, SettlementReceipt

LOG_PATH = os.path.join("logs", "settlement.log")
INITIAL_LIQUIDITY_USD = 48_920_450.00


class ConnectionPoolExhaustedError(RuntimeError):
    """Raised when all database pool connections are leaked or busy."""


class NexusPayInfrastructure:
    def __init__(self, max_connections: int = 5):
        self.max_connections = max_connections
        self._active_connections = 0
        self._lock = threading.Lock()
        self._idempotency_ledger: Dict[str, SettlementReceipt] = {}
        self._recent_outcomes: deque[tuple[float, bool, float]] = deque(maxlen=100)
        self.available_liquidity_usd: float = INITIAL_LIQUIDITY_USD
        os.makedirs("logs", exist_ok=True)

    def log_event(self, level: str, event: str, **fields) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "service": "nexuspay-settlement-gateway",
            "event": event,
            **fields,
        }
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def acquire_connection(self) -> int:
        with self._lock:
            if self._active_connections >= self.max_connections:
                self.log_event(
                    "ERROR",
                    "db_pool_exhausted",
                    status=503,
                    active_connections=self._active_connections,
                    max_connections=self.max_connections,
                    error="Timeout acquiring database connection from NexusPay pool",
                )
                raise ConnectionPoolExhaustedError(
                    f"503 GATEWAY_POOL_EXHAUSTED ({self._active_connections}/{self.max_connections} connections locked)"
                )
            self._active_connections += 1
            return self._active_connections

    def release_connection(self) -> None:
        with self._lock:
            if self._active_connections > 0:
                self._active_connections -= 1

    def deduct_liquidity(self, amount_usd: float) -> float:
        with self._lock:
            self.available_liquidity_usd = round(max(0.0, self.available_liquidity_usd - amount_usd), 2)
            return self.available_liquidity_usd

    def record_transaction(self, latency_ms: float, success: bool) -> None:
        self._recent_outcomes.append((time.time(), success, latency_ms))

    def get_idempotent_receipt(self, key: str) -> Optional[SettlementReceipt]:
        return self._idempotency_ledger.get(key)

    def save_idempotent_receipt(self, key: str, receipt: SettlementReceipt) -> None:
        self._idempotency_ledger[key] = receipt

    def reset_state(self) -> None:
        with self._lock:
            self._active_connections = 0
            self._idempotency_ledger.clear()
            self._recent_outcomes.clear()
            self.available_liquidity_usd = INITIAL_LIQUIDITY_USD
        open(LOG_PATH, "w", encoding="utf-8").close()
        self.log_event("INFO", "infrastructure_reset", active_connections=0, liquidity_usd=self.available_liquidity_usd)

    def snapshot_golden_signals(self) -> GoldenSignalsSnapshot:
        now = time.time()
        window = [item for item in self._recent_outcomes if now - item[0] <= 60.0]
        total = len(window)
        failures = sum(1 for _, ok, _ in window if not ok)
        latencies = sorted(lat for _, _, lat in window) or [38.0]

        p95_idx = min(len(latencies) - 1, int(len(latencies) * 0.95))
        p95_latency = round(latencies[p95_idx], 2)
        error_rate = round((failures / total) * 100.0, 2) if total > 0 else 0.0
        throughput = round(total / 60.0, 2)
        saturation = round((self._active_connections / self.max_connections) * 100.0, 1)

        slo_burn_rate = round((error_rate / 0.1), 2) if error_rate > 0 else 0.0
        healthy = saturation < 90.0 and error_rate < 5.0

        return GoldenSignalsSnapshot(
            p95_latency_ms=p95_latency,
            throughput_rps=throughput,
            error_rate_pct=error_rate,
            pool_saturation_pct=saturation,
            active_connections=self._active_connections,
            max_connections=self.max_connections,
            slo_burn_rate=slo_burn_rate,
            healthy=healthy,
        )


infra = NexusPayInfrastructure(max_connections=5)