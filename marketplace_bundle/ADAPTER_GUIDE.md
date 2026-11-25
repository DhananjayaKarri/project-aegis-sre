# Reusable AI Factory Adapter Guide (Anonymized Marketplace Artifact)

## Purpose
This bundle packages the **Autonomous Self-Healing & Guardrailed Multi-Agent SRE Factory** in a client-anonymized form so other engineering squads can reuse the orchestration pattern, inter-agent contract guardrails, sandbox verification loop, and evaluation harness.

## Included Components
- `aegis_agents/agent.py`: Tiered `LITE` vs. `REASONING` sub-agents under a `SequentialAgent` orchestrator (`ParallelAgent` triage + `LoopAgent` patch/verify loop + post-mortem writer).
- `aegis_agents/callbacks.py`: Pre-handoff state contract guardrails (`PASS` / `BLOCKED` / `HELD_FOR_APPROVAL`) and trace persistence.
- `aegis_agents/tools.py`: Write-ahead `.bak` backups, code-fence sanitization, isolated sandbox stress testing, and `AUTONOMOUS` vs. `APPROVAL_GATE` promotion controls.
- `control_plane/watcher.py` & `control_plane/event_bus.py`: SLO breach monitoring, SHA-256 incident idempotency lock, multi-model failover, circuit-breaker fallback, and per-agent token/MTTR/ROI tracking.
- `evals/run_evals.py`: Automated multi-scenario evaluation runner outputting a JSON scorecard.

## Adapting to Another Service (3 Steps)
1. **Swap the Domain Telemetry & Target Service:** Replace the target handler and golden-signals snapshot adapter with your target service's health metrics, structured log reader, and resource/retry handler.
2. **Tune Guardrail & Autonomy Policy:** Adjust required handoff state keys (`REQUIRED_HANDOFF_KEYS`), sandbox stress test parameters, and default mode (`AUTONOMOUS` vs. `APPROVAL_GATE`).
3. **Run Evals & Track Cost per Incident:** Execute `python evals/run_evals.py` and record token counts, MTTR, and human hours saved per incident run.
