# Project Aegis-SRE: Autonomous Self-Healing Payment Gateway

Project Aegis-SRE is a multi-agent Site Reliability Engineering (SRE) control plane paired with **NexusPay**, a simulated B2B cross-border wire settlement gateway. Built on Google's Agent Development Kit (ADK) and Gemini models, the system continuously tracks Google SRE's Four Golden Signals, isolates production bottlenecks, validates code patches in an isolated concurrency sandbox, and generates blameless post-mortem documentation.

---

## System Overview

The platform operates across two synchronized browser views served by a unified FastAPI backend:

* **NexusPay Treasury Portal (`/`):** Allows enterprise merchants to execute ISO-20022 style wire transfers, tracks live treasury liquidity (`$48,920,450.00` baseline), enforces `X-Idempotency-Key` replay safety to prevent double deductions, and renders a live settlement ledger.
* **Aegis-SRE Command Center (`/admin`):** Streams live Four Golden Signals metrics (p95 Latency, Traffic, 5xx Error Rate, and Connection Pool Saturation) over Server-Sent Events (SSE), visualizes parent-to-child ADK agent execution in real time, displays unified code diffs, and exports blameless post-mortem reports.

### The Incident Scenario
Under normal load, NexusPay settles wires within ~40–75 ms using a 5-connection database pool. However, high-value compliance wires (`>= $10,000` or flagged for audit) trigger an early return in `nexuspay_service/settlement_handler.py` without releasing the acquired database connection. Once all 5 connections leak, subsequent wires fail with `HTTP 503 GATEWAY_POOL_EXHAUSTED`, breaching the 99.9% SLO burn-rate threshold and waking the Aegis-SRE control plane.

---

## Multi-Agent Architecture

`SRE_Commander` (`SequentialAgent`) coordinates five specialized child agents over shared ADK session state, protected by a SHA-256 incident idempotency lock so duplicate alerts never trigger overlapping remediation runs:

1. **Stage 1 — Parallel Triage (`ParallelAgent`):**
   * `TelemetrySignalAgent`: Evaluates live Golden Signals and error-budget burn rate.
   * `LogForensicsAgent`: Parses structured JSON logs (`logs/settlement.log`) to trace `compliance_hold_early_return_leaked_connection` events to the exact failing code path.
2. **Stage 2 — Guardrailed Remediation (`LoopAgent`):**
   * `RootCauseArchitect`: Reads `nexuspay_service/settlement_handler.py`, writes a write-ahead `.bak` snapshot, generates a `try...finally` connection-release fix, and stages it to `settlement_handler.candidate.py`.
   * `GuardrailVerifier`: Compiles the staged candidate and runs a 15-wire concurrent stress test against an isolated `NexusPayInfrastructure` sandbox (ensuring zero impact on live treasury balances).
3. **Stage 3 — Blameless Post-Mortem:**
   * `PostMortemWriter`: Compiles the timeline, root cause, verification proof, and financial safety summary into Markdown and writes both the report and the full parent-child JSON trace to `demo_artifacts/`.

### Operational Modes
* **Full Autonomous (`AUTONOMOUS`):** Once the 15-wire sandbox test passes with zero leaked connections, `GuardrailVerifier` atomically swaps the candidate into production and hot-reloads the live module in memory.
* **Human Approval Gate (`APPROVAL_GATE`):** After passing sandbox verification, the pipeline pauses before modifying live production code, surfaces the unified diff in the Command Center, and waits for an engineer to click **Approve & Deploy Staged Patch**.

---

## Repository Structure

```text
project-aegis-sre/
├── aegis_agents/
│   ├── agent.py              # ADK Sequential, Parallel, and Loop agent topology + model failover
│   ├── callbacks.py          # Parent-child handoff telemetry & demo_artifacts persistence
│   └── tools.py              # Code inspection, .bak snapshots, shadow staging & sandbox stress tests
├── control_plane/
│   ├── app.py                # FastAPI routes, SSE stream, chaos surge trigger & dual-route UI host
│   ├── event_bus.py          # Async pub/sub event broadcaster for live dashboard updates
│   └── watcher.py            # Background SLO monitor, SHA-256 incident lock & circuit breaker
├── domain/
│   └── schemas.py            # Shared Pydantic contracts for signals, receipts, and handoff events
├── frontend/
│   └── index.html            # Customer Treasury Portal (/) and Admin SRE Command Center (/admin)
├── nexuspay_service/
│   ├── pool_manager.py       # Connection pool tracking, liquidity ledger & structured JSON logging
│   └── settlement_handler.py # Wire settlement service containing the intentional resource leak
├── demo_artifacts/           # Recorded parent-child JSON traces and generated Markdown post-mortems
├── Dockerfile                # Container specification for Cloud Run deployment
├── requirements.txt          # Python dependencies
└── .env.example              # Environment variable template

## Local Setup & Execution

Follow these steps to set up and run Project Aegis SRE locally.

### Prerequisites

Ensure the following are installed on your system:

- **Python 3.10 or higher**
- **Google AI Studio API Key** (`GOOGLE_API_KEY`)

### Installation

**1. Clone the repository**

```bash
git clone https://github.com/DhananjayaKarri/project-aegis-sre.git
cd project-aegis-sre
```

**2. Create and activate a virtual environment**

```bash
python -m venv .venv
```

Activate the virtual environment:

```bash
# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Configure environment variables**

Create a `.env` file using the example configuration:

```bash
cp .env.example .env
```

Open the `.env` file and configure your API key:

```env
GOOGLE_API_KEY=your_api_key_here
GOOGLE_GENAI_USE_VERTEXAI=FALSE
```

Replace `your_api_key_here` with your actual Google AI Studio API key.

### Running the Application

**1. Start the FastAPI server**

```bash
uvicorn control_plane.app:app --host 127.0.0.1 --port 8080
```

**2. Open the application in two browser windows side by side**

| Application | URL |
|---|---|
| Customer Portal | http://127.0.0.1:8080/ |
| Admin SRE Console | http://127.0.0.1:8080/admin |

### Demo Walkthrough

Follow these steps to explore the application's core capabilities.

#### 1. Verify Normal Operation

- Navigate to the **Customer Portal** (`/`).
- Click **Submit Wire Transfer**.
- Verify that the wire transfer settles successfully (`200 OK`) and deducts the corresponding amount from Available Settlement Liquidity.
- Click **Replay Last Wire** to verify that idempotency protection prevents duplicate deductions.

#### 2. Trigger Outage & Self-Healing

- Navigate to the **Admin SRE Console** (`/admin`).
- Select either:
  - **Mode: Full Autonomous**
  - **Mode: Human Approval Gate**
- Click **Simulate Settlement Surge (Chaos)**.
- Observe the self-healing workflow:
  1. Connection pool saturation reaches 100% (5/5).
  2. The ADK pipeline gets triggered.
  3. The `try...finally` patch is staged.
  4. Pool health is restored to 0% saturation (0/5).

#### 3. Reset Demo State

- Click **Reset Demo Baseline** in the top toolbar.
- This restores the original buggy `settlement_handler.py` baseline.
- Treasury liquidity is also reset to its initial state, preparing the application for the next demonstration.

---

**You're all set!** Follow the walkthrough to experience Project Aegis SRE's autonomous incident detection and self-healing capabilities.
