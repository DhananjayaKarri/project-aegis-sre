from __future__ import annotations
import difflib
import importlib
import importlib.util
import os
import py_compile
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from google.adk.tools import ToolContext
from domain.schemas import SettlementRequest
from nexuspay_service.pool_manager import LOG_PATH, infra

TARGET_FILE = os.path.join("nexuspay_service", "settlement_handler.py")
CANDIDATE_FILE = os.path.join("nexuspay_service", "settlement_handler.candidate.py")
BASELINE_BACKUP = os.path.join("nexuspay_service", "settlement_handler.baseline.bak")

# Keep baseline backup synchronized with the current buggy baseline on startup
if os.path.exists(TARGET_FILE):
    with open(TARGET_FILE, "r", encoding="utf-8") as _tf:
        _target_src = _tf.read()
    if "finally:" not in _target_src:
        shutil.copy2(TARGET_FILE, BASELINE_BACKUP)


def fetch_golden_signals() -> Dict[str, Any]:
    """Fetches real-time Google SRE Four Golden Signals (Latency, Traffic, Errors, Saturation)."""
    snapshot = infra.snapshot_golden_signals()
    return snapshot.model_dump()


def inspect_service_logs(tail_lines: int = 35) -> str:
    """Reads the most recent structured JSON telemetry and error logs from NexusPay."""
    if not os.path.exists(LOG_PATH):
        return "No service logs found."
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()[-tail_lines:]
    return "".join(lines) if lines else "Service log is currently empty."


def read_target_source_code() -> str:
    """Reads the live source code of nexuspay_service/settlement_handler.py."""
    with open(TARGET_FILE, "r", encoding="utf-8") as f:
        return f.read()


def stage_shadow_patch(proposed_python_code: str, tool_context: ToolContext) -> str:
    """
    Writes a write-ahead .bak snapshot and stages proposed_python_code into a shadow
    candidate file without touching live production code yet.
    """
    incident_hash = str(tool_context.state.get("incident_hash", "manual")).replace(":", "_")
    bak_path = f"{TARGET_FILE}.{incident_hash[:12]}.bak"

    with open(TARGET_FILE, "r", encoding="utf-8") as f:
        original_code = f.read()

    with open(bak_path, "w", encoding="utf-8") as f:
        f.write(original_code)

    with open(CANDIDATE_FILE, "w", encoding="utf-8") as f:
        f.write(proposed_python_code)

    diff_lines = list(
        difflib.unified_diff(
            original_code.splitlines(),
            proposed_python_code.splitlines(),
            fromfile="settlement_handler.py (broken)",
            tofile="settlement_handler.py (patched)",
            lineterm="",
        )
    )
    unified_diff_text = "\n".join(diff_lines)
    tool_context.state["patch_diff"] = unified_diff_text
    tool_context.state["backup_path"] = bak_path
    return f"Shadow patch staged at {CANDIDATE_FILE} (backup saved at {bak_path}). Diff:\n{unified_diff_text}"


def verify_and_promote_patch(tool_context: ToolContext) -> str:
    """
    Runs syntax compilation and a 15-request concurrent connection-pool stress test
    against the staged shadow candidate. Promotes to live production ONLY if all checks pass.
    """
    if not os.path.exists(CANDIDATE_FILE):
        return "VERIFICATION_FAILED: No staged candidate file found. Call stage_shadow_patch first."

    # Guardrail 1: Python bytecode syntax validation
    try:
        py_compile.compile(CANDIDATE_FILE, doraise=True)
    except py_compile.PyCompileError as exc:
        return f"VERIFICATION_FAILED (SyntaxError): {exc}"

    # Guardrail 2: Load shadow candidate in isolation and run 15 concurrent compliance wires
    try:
        spec = importlib.util.spec_from_file_location("shadow_handler", CANDIDATE_FILE)
        shadow_mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(shadow_mod)

        infra.reset_state()

        def _stress_call(idx: int):
            req = SettlementRequest(
                merchant_id=f"VERIFY-{idx}",
                recipient_iban="DE89370400440532013000",
                amount_usd=15000.00,
                simulate_validation_warning=True,
            )
            return shadow_mod.execute_settlement(req, idempotency_key=f"smoke-test-{idx}")

        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(_stress_call, range(15)))

        post_metrics = infra.snapshot_golden_signals()
        if post_metrics.active_connections != 0:
            return (
                f"VERIFICATION_FAILED (ConnectionLeak): Expected 0 active connections after test, "
                f"found {post_metrics.active_connections}/{post_metrics.max_connections} still locked!"
            )
    except Exception as exc:
        return f"VERIFICATION_FAILED (RuntimeError during concurrency test): {exc}"

    # Guardrail 3: Check Human-in-the-Loop Approval Gate mode
    if tool_context.state.get("operational_mode") == "APPROVAL_GATE" and not tool_context.state.get("human_approved", False):
        tool_context.state["awaiting_human_approval"] = True
        tool_context.actions.escalate = True
        infra.reset_state()
        return (
            "APPROVAL_REQUIRED: Shadow patch passed 15/15 concurrency stress tests with 0 leaked connections. "
            "Paused at Human-in-the-Loop Approval Gate awaiting SRE click to promote."
        )

    # Atomic swap into live module & hot-reload in memory
    os.replace(CANDIDATE_FILE, TARGET_FILE)
    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)
    infra.reset_state()

    tool_context.state["patch_promoted"] = True
    tool_context.actions.escalate = True
    return (
        "VERIFICATION_PASSED_AND_PROMOTED: Shadow patch passed 15/15 concurrent compliance wires, "
        "atomic swap completed, live module hot-reloaded, and pool saturation is 0.0%."
    )


def promote_staged_candidate_manually() -> str:
    """Called when an SRE clicks 'Approve & Deploy Patch' in APPROVAL_GATE mode."""
    if not os.path.exists(CANDIDATE_FILE):
        return "No staged candidate patch waiting for approval."
    os.replace(CANDIDATE_FILE, TARGET_FILE)
    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)
    infra.reset_state()
    return "Human SRE approved patch: atomic swap completed and NexusPay gateway hot-reloaded."


def restore_baseline_bug() -> str:
    """Restores the original buggy settlement_handler.py so the live demo can be re-run cleanly."""
    if os.path.exists(BASELINE_BACKUP):
        shutil.copy2(BASELINE_BACKUP, TARGET_FILE)
    if os.path.exists(CANDIDATE_FILE):
        os.remove(CANDIDATE_FILE)
    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)
    infra.reset_state()
    return "Restored baseline buggy settlement_handler.py and cleared pool state."