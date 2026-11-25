from __future__ import annotations
import difflib
import importlib
import importlib.util
import os
import py_compile
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Tuple

from google.adk.tools import ToolContext
from domain.schemas import SettlementRequest
from nexuspay_service.pool_manager import INITIAL_LIQUIDITY_USD, LOG_PATH, NexusPayInfrastructure, infra

TARGET_FILE = os.path.join("nexuspay_service", "settlement_handler.py")
CANDIDATE_FILE = os.path.join("nexuspay_service", "settlement_handler.candidate.py")
BASELINE_BACKUP = os.path.join("nexuspay_service", "settlement_handler.baseline.bak")

# Keep baseline backup synchronized with the buggy baseline on startup
if os.path.exists(TARGET_FILE):
    with open(TARGET_FILE, "r", encoding="utf-8") as _tf:
        _target_src = _tf.read()
    if "finally:" not in _target_src:
        shutil.copy2(TARGET_FILE, BASELINE_BACKUP)


def _strip_code_fences(code: str) -> str:
    cleaned = code.strip()
    cleaned = re.sub(r"^```(?:python|py)?\s*\n?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\n?```\s*$", "", cleaned)
    return cleaned.strip() + "\n"


def build_deterministic_patched_handler() -> str:
    """Produces a clean try...finally patched version of settlement_handler.py."""
    return '''from __future__ import annotations
import time
import uuid
from domain.schemas import SettlementReceipt, SettlementRequest
from nexuspay_service.pool_manager import infra


def execute_settlement(req: SettlementRequest, idempotency_key: str) -> SettlementReceipt:
    """Processes a B2B wire transfer through EnterpriseSettlementGateway Core."""
    existing = infra.get_idempotent_receipt(idempotency_key)
    if existing is not None:
        replay = existing.model_copy(update={"status": "IDEMPOTENT_REPLAY"})
        infra.log_event("INFO", "idempotent_replay_prevented", idempotency_key=idempotency_key)
        return replay

    start = time.perf_counter()
    conn_id = infra.acquire_connection()
    try:
        if req.simulate_validation_warning or req.amount_usd >= 10000.0:
            time.sleep(0.06)
            latency_ms = round((time.perf_counter() - start) * 1000, 2)
            infra.deduct_liquidity(req.amount_usd)
            infra.log_event(
                "INFO",
                "compliance_hold_settled_safely",
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
                detail="Settled with compliance audit flag (connection safely released)",
            )
            infra.save_idempotent_receipt(idempotency_key, receipt)
            return receipt

        time.sleep(0.04)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        infra.deduct_liquidity(req.amount_usd)
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
    finally:
        infra.release_connection()
'''


def fetch_golden_signals() -> Dict[str, Any]:
    """Fetches real-time Google SRE Four Golden Signals (Latency, Traffic, Errors, Saturation)."""
    return infra.snapshot_golden_signals().model_dump()


def inspect_service_logs(tail_lines: int = 35) -> str:
    """Reads the most recent structured JSON telemetry and error logs from EnterpriseSettlementGateway."""
    if not os.path.exists(LOG_PATH):
        return "No service logs found."
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()[-tail_lines:]
    return "".join(lines) if lines else "Service log is currently empty."


def read_target_source_code() -> str:
    """Reads the live source code of nexuspay_service/settlement_handler.py."""
    with open(TARGET_FILE, "r", encoding="utf-8") as f:
        return f.read()


def stage_shadow_patch_direct(proposed_python_code: str, incident_hash: str) -> Tuple[str, str]:
    """
    Guardrailed patch staging: enforces non-empty diff and valid Python syntax before
    writing a .bak backup and staging settlement_handler.candidate.py.
    """
    safe_hash = str(incident_hash or "manual").replace(":", "_")[:16]
    bak_path = f"{TARGET_FILE}.{safe_hash}.bak"
    cleaned_code = _strip_code_fences(proposed_python_code)

    with open(TARGET_FILE, "r", encoding="utf-8") as f:
        original_code = f.read()

    if cleaned_code.strip() == original_code.strip():
        raise ValueError("GUARDRAIL_BLOCKED_NOOP_PATCH: Proposed code is identical to target source.")

    # Pre-stage syntax check so broken code never enters candidate staging
    compile(cleaned_code, CANDIDATE_FILE, "exec")

    with open(bak_path, "w", encoding="utf-8") as f:
        f.write(original_code)

    with open(CANDIDATE_FILE, "w", encoding="utf-8") as f:
        f.write(cleaned_code)

    diff_lines = list(
        difflib.unified_diff(
            original_code.splitlines(),
            cleaned_code.splitlines(),
            fromfile="settlement_handler.py (broken)",
            tofile="settlement_handler.py (patched)",
            lineterm="",
        )
    )
    unified_diff_text = "\n".join(diff_lines)
    return unified_diff_text, bak_path


def stage_shadow_patch(proposed_python_code: str, tool_context: ToolContext) -> str:
    """
    Writes a write-ahead .bak snapshot and stages proposed_python_code into a shadow
    candidate file without touching live production code yet.
    """
    incident_hash = str(tool_context.state.get("incident_hash", "manual"))
    try:
        unified_diff_text, bak_path = stage_shadow_patch_direct(proposed_python_code, incident_hash)
    except Exception as exc:
        tool_context.state["guardrail_last_error"] = str(exc)
        return f"STAGE_BLOCKED: {exc}"

    tool_context.state["patch_diff"] = unified_diff_text
    tool_context.state["backup_path"] = bak_path
    tool_context.state["patch_staged"] = True
    return f"Shadow patch staged at {CANDIDATE_FILE} (backup saved at {bak_path}). Diff:\n{unified_diff_text}"


def verify_and_promote_patch_direct(operational_mode: str, human_approved: bool = False) -> Tuple[str, bool, bool]:
    """
    Validates staged candidate via bytecode compilation and an isolated 15-wire concurrency stress test.
    Returns (status_message, passed, awaiting_approval).
    """
    if not os.path.exists(CANDIDATE_FILE):
        return "VERIFICATION_FAILED: No staged candidate file found. Call stage_shadow_patch first.", False, False

    try:
        py_compile.compile(CANDIDATE_FILE, doraise=True)
    except py_compile.PyCompileError as exc:
        return f"VERIFICATION_FAILED (SyntaxError): {exc}", False, False

    try:
        spec = importlib.util.spec_from_file_location("shadow_handler", CANDIDATE_FILE)
        shadow_mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(shadow_mod)

        sandbox_infra = NexusPayInfrastructure(max_connections=5, write_logs=False)
        shadow_mod.infra = sandbox_infra

        def _stress_call(idx: int):
            req = SettlementRequest(
                merchant_id=f"VERIFY-{idx}",
                recipient_iban="ANON000000000000000000",
                amount_usd=15000.00,
                simulate_validation_warning=True,
            )
            return shadow_mod.execute_settlement(req, idempotency_key=f"smoke-test-{idx}")

        with ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(_stress_call, range(15)))

        post_metrics = sandbox_infra.snapshot_golden_signals()
        if len(results) != 15 or post_metrics.active_connections != 0:
            return (
                f"VERIFICATION_FAILED (ConnectionLeak): Expected 0 active connections after 15 wires, "
                f"found {post_metrics.active_connections}/{post_metrics.max_connections} still locked!",
                False,
                False,
            )
        if sandbox_infra.available_liquidity_usd >= INITIAL_LIQUIDITY_USD:
            return (
                "VERIFICATION_FAILED (LiquidityRegression): Candidate patch stopped calling infra.deduct_liquidity().",
                False,
                False,
            )
    except Exception as exc:
        return f"VERIFICATION_FAILED (RuntimeError during concurrency test): {exc}", False, False

    if operational_mode == "APPROVAL_GATE" and not human_approved:
        return (
            "APPROVAL_REQUIRED: Shadow patch passed 15/15 isolated concurrency wires with 0 leaked connections. "
            "Live production code is untouched; awaiting SRE click on 'Approve & Deploy Staged Patch'.",
            True,
            True,
        )

    os.replace(CANDIDATE_FILE, TARGET_FILE)
    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)
    infra.release_leaked_connections()
    return (
        "VERIFICATION_PASSED_AND_PROMOTED: Shadow patch passed 15/15 isolated compliance wires, "
        "atomic swap completed, live module hot-reloaded, and connection pool recovered to 0.0% (0/5).",
        True,
        False,
    )


def verify_and_promote_patch(tool_context: ToolContext) -> str:
    """ADK Tool wrapper for shadow verification and promotion."""
    mode = str(tool_context.state.get("operational_mode", "AUTONOMOUS"))
    approved = bool(tool_context.state.get("human_approved", False))
    msg, passed, awaiting = verify_and_promote_patch_direct(mode, approved)

    tool_context.state["sandbox_verified"] = passed
    if awaiting:
        tool_context.state["awaiting_human_approval"] = True
        tool_context.actions.escalate = True
    elif passed:
        tool_context.state["patch_promoted"] = True
        tool_context.state["awaiting_human_approval"] = False
        tool_context.actions.escalate = True
    return msg


def promote_staged_candidate_manually() -> str:
    """Called when an SRE clicks 'Approve & Deploy Staged Patch' in APPROVAL_GATE mode."""
    if not os.path.exists(CANDIDATE_FILE):
        return "No staged candidate patch waiting for approval."
    os.replace(CANDIDATE_FILE, TARGET_FILE)
    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)
    infra.release_leaked_connections()
    return "Human SRE approved patch: atomic swap completed, live gateway hot-reloaded, and pool recovered to 0% (0/5)."


def restore_baseline_bug() -> str:
    """Restores the original buggy settlement_handler.py so the live demo can be re-run cleanly."""
    if os.path.exists(BASELINE_BACKUP):
        shutil.copy2(BASELINE_BACKUP, TARGET_FILE)
    if os.path.exists(CANDIDATE_FILE):
        os.remove(CANDIDATE_FILE)
    import nexuspay_service.settlement_handler as live_handler
    importlib.reload(live_handler)
    infra.reset_state()
    return "Restored baseline buggy settlement_handler.py and reset treasury & pool state."