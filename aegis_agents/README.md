# Project Aegis-SRE — Autonomous Multi-Agent Self-Healing Gateway (NexusPay)

An autonomous Site Reliability Engineering (SRE) platform built with **Google ADK (Agent Development Kit)** and **Gemini 2.5** that monitors Google SRE's **Four Golden Signals**, diagnoses production connection-pool saturation, verifies code patches behind a concurrency stress-test guardrail, and generates blameless post-mortems.

## Architecture
- **`domain/`**: Decoupled Pydantic schemas (`GoldenSignalsSnapshot`, `SettlementRequest`, `AgentHandoffEvent`).
- **`nexuspay_service/`**: Target B2B payment gateway with connection-pool management and `X-Idempotency-Key` replay protection.
- **`aegis_agents/`**:
  - **Parent Orchestrator (`SRE_Commander` — `SequentialAgent`)**: Coordinates shared session state across stages.
  - **Stage 1 (`TriageSwarm` — `ParallelAgent`)**: `TelemetrySignalAgent` + `LogForensicsAgent` (`gemini-2.5-flash`).
  - **Stage 2 (`GuardrailHealingLoop` — `LoopAgent`)**: `RootCauseArchitect` (`gemini-2.5-pro`) + `GuardrailVerifier` (syntax + 15-wire concurrency smoke test + atomic `.bak` rollback).
  - **Stage 3 (`PostMortemWriter`)**: Generates Google SRE Blameless Post-Mortems and exports Parent–Child JSON traces to `demo_artifacts/`.
- **`control_plane/`**: SLO error-budget burn-rate watcher, SHA-256 incident lock, and FastAPI SSE event bus.
- **`frontend/`**: Dual-route web interface (`/` Customer Treasury Portal & `/admin` SRE Command Center).

## Quick Start (Local)
1. `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and set `GOOGLE_API_KEY`.
3. Run `uvicorn control_plane.app:app --host 127.0.0.1 --port 8080`.
4. Open `http://127.0.0.1:8080/` (Customer Portal) and `http://127.0.0.1:8080/admin` (Admin SRE Console).