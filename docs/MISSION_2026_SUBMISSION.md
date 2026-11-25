# EPAM AI/Run Mission 2026 — Milestone 2 Submission Pack: Project Aegis-SRE

## 1. Solution (`SOL`)
**Project Aegis-SRE** is an autonomous, guardrail-enforced Site Reliability Engineering (SRE) control plane built on Google's Agent Development Kit (ADK) and Gemini models, paired with **NexusPay**, an enterprise B2B wire settlement gateway.

Rather than relying on ad-hoc chat prompting, the factory monitors live service health, isolates production faults, generates and tests code fixes inside an isolated concurrency sandbox, enforces human or autonomous promotion gates, and produces blameless post-mortems and audit traces automatically.

Key capabilities delivered:
* **Customer Treasury Portal (`/`) & Admin SRE Command Center (`/admin`):** Dual-route FastAPI application demonstrating normal B2B wire execution, live liquidity tracking (`$48,920,450.00` baseline), `X-Idempotency-Key` duplicate-charge protection (`$0.00` extra deduction on replay), and real-time SRE recovery over Server-Sent Events (SSE).
* **Tiered Lite vs. Reasoning Agents:** Separates lightweight triage/gate steps (`LITE` tier) from deeper code-patch and post-mortem synthesis (`REASONING` tier) to optimize token consumption.
* **Multi-Scenario Fault Coverage & Automated Evals:** Covers both a **Compliance Hold Connection Pool Leak** (`503 GATEWAY_POOL_EXHAUSTED`) and an **Upstream FX Retry Amplification Storm** (`upstream_retry_storm`), validated via `evals/run_evals.py`.
* **Client-Anonymized Marketplace Export:** `scripts/export_marketplace_bundle.py` packages reusable factory modules, guardrails, and an adapter guide with sensitive identifiers and local paths scrubbed.

---

## 2. Architecture (`ARC`)
### Orchestration & Multi-Agent Topology
* **Parent Orchestrator (`SRE_Commander` — `SequentialAgent`):** Coordinates execution across shared session state and enforces a SHA-256 incident idempotency lock (`_active_incident_hash` / `_resolved_hashes`) so duplicate alerts do not trigger overlapping runs.
* **Stage 1 — Parallel Triage (`TriageSwarm` — `ParallelAgent`):**
  * `TelemetrySignalAgent` (`LITE` tier): Evaluates Google SRE's Four Golden Signals (p95 Latency, Throughput, 5xx Error Rate, Pool Saturation) and SLO burn rate.
  * `LogForensicsAgent` (`LITE` tier): Inspects structured JSON logs (`logs/settlement.log`) to pinpoint early returns leaking pool connections.
* **Stage 2 — Guardrailed Patch & Verification Loop (`GuardrailHealingLoop` — `LoopAgent`):**
  * `RootCauseArchitect` (`REASONING` tier): Inspects `nexuspay_service/settlement_handler.py`, writes a write-ahead `.bak` backup, stages a `try...finally` connection-release fix in `settlement_handler.candidate.py` (rejecting no-op or invalid syntax patches), and generates a unified diff.
  * `GuardrailVerifier` (`LITE` tier): Compiles the candidate and executes a 15-wire concurrent stress test against an isolated `NexusPayInfrastructure(max_connections=5, write_logs=False)` sandbox so live treasury balances and logs remain untouched during verification.
* **Stage 3 — Post-Mortem & Artifact Export (`PostMortemWriter` — `REASONING` tier):** Generates a structured Markdown Blameless Post-Mortem and persists both Markdown reports and JSON handoff traces to `demo_artifacts/`.

### Autonomy Controls & Resilience
* **Two Operating Modes:**
  * `AUTONOMOUS`: Automatically promotes verified patches via atomic file swap (`os.replace`) and in-memory module hot-reload (`importlib.reload`), releasing leaked pool slots back to `0% (0/5)`.
  * `APPROVAL_GATE`: Pauses after passing the 15-wire sandbox stress test (`WAITING_APPROVAL` / `HELD_FOR_APPROVAL`), surfaces the unified diff in `/admin`, and requires explicit human approval (`/api/admin/approve`) before modifying live production code.
* **Model Failover & Circuit Breaker:** Cycles across configured Gemini models with per-model timeouts and falls back to a deterministic SRE circuit-breaker runbook if external LLM endpoints return `503` or `429`.

---

## 3. Demo (`DEM`)
### End-to-End Walkthrough
1. **Baseline & Idempotency Check (`/`):** Submit a high-value wire transfer (`>= $10,000`). Confirm `200 SETTLED`, live deduction from **Available Settlement Liquidity**, and `IDEMPOTENT_REPLAY` (`$0.00` extra deduction) when replaying the same idempotency key.
2. **Pool Exhaustion & SLO Breach (`/admin`):** Trigger **Simulate Settlement Surge (Chaos)** (or send wires past the 5-connection pool limit). Observe pool saturation reach `100% (5/5)` and `SRE_Commander` acquire the SHA-256 incident lock.
3. **Autonomous vs. Approval Gate Recovery:**
   * In `AUTONOMOUS` mode, watch `1a`, `1b`, `2a`, `2b`, and `3` complete, inspect the unified diff and blameless post-mortem, and confirm pool saturation returns to `0% (0/5)`.
   * In `APPROVAL_GATE` mode, confirm `GuardrailVerifier` pauses for review and promotes cleanly upon approval.
4. **Baseline Reset:** Click **Reset Demo Baseline** to restore the original buggy baseline handler and reset liquidity and pool state for repeatable demonstrations.

---

## 4. Outcomes (`OUT`) — Path B Measurement & Cost Tracking
*Note on Measurement Path:* Following Mission 2026 **Path B**, this submission reports observed workflow telemetry, per-agent token accounting, MTTR, and engineering/operational safeguards without claiming unverified organization-wide or multi-sprint commercial baselines.

### Tracked Metrics & Artifacts
| Metric / Dimension | How Measured / Captured | Artifact / Location |
| :--- | :--- | :--- |
| **Per-Agent & Total Tokens** | Tracked per agent (`tokens_in`, `tokens_out`, `total_tokens`, `agent_tier`, `model_used`) and rolled up per incident in `IncidentCostAndROI` | `control_plane/watcher.py`, `demo_artifacts/incident_*_trace.json` |
| **Estimated AI Cost per Incident** | Computed from incident token volume (`COST_PER_MILLION_TOKENS_USD`) and attached to status/SSE/post-mortem outputs | `control_plane/watcher.py`, `demo_artifacts/POSTMORTEM_*.md` |
| **MTTR & Time Saved** | Wall-clock remediation duration (`mttr_seconds`) compared against a `45.0`-minute manual triage/patch/test baseline (`estimated_hours_saved`) | `control_plane/watcher.py`, `demo_artifacts/POSTMORTEM_*.md` |
| **Multi-Scenario Eval Pass Rate** | Automated checks across `connection_pool_leak` and `upstream_retry_storm` (pool saturation, sandbox 15-wire test, idempotency safety, post-fix recovery) | `evals/run_evals.py`, `demo_artifacts/eval_report.json` |
| **Marketplace Reuse Artifact** | Client-anonymized factory bundle with adapter guide | `scripts/export_marketplace_bundle.py`, `demo_artifacts/project_aegis_sre_marketplace_bundle.zip` |

---

## 5. Lessons Learned (`LRN`)
* **External Model Availability & Failover Matter in Live Demos:** Relying on a single preview or high-demand model ID without timeouts, model fallback, and circuit-breaker guardrails can stall live runs during API `404` or `503` spikes.
* **Sandbox Isolation Prevents State Pollution:** Running concurrency stress tests against a dedicated `NexusPayInfrastructure(..., write_logs=False)` instance avoids wiping live customer treasury balances or corrupting logs mid-incident.
* **Sequenced Contracts Prevent Integration Drift:** Defining shared Pydantic schemas (`domain/schemas.py`) first and splitting work across backend agents, control-plane telemetry, UI, and evals keeps parallel team tasks aligned.

---

## 6. Plans (`PLN`)
1. **Complete Per-Agent Token UI Badge (Task 5):** Integrate the per-agent token badge between each agent name and status pill plus the incident ROI strip in `frontend/index.html`.
2. **Cloud Run Deployment & Governance Sign-Off:** Deploy to Google Cloud Run (`--max-instances 1 --no-cpu-throttling --timeout 3600`) once mentor/project approval is confirmed, and present using the 5-beat cadence (*Approach, Progress, Impact, Demo, Gaps & Support Needed*).
3. **Team Adoption & Marketplace Submission:** Share the anonymized bundle (`project_aegis_sre_marketplace_bundle.zip`) with peers/other squads and submit client-anonymized factory artifacts to the AI Assets Marketplace.