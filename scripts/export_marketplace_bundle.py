from __future__ import annotations
import os
import re
import shutil
import zipfile
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parent.parent
BUNDLE_DIR = ROOT_DIR / "marketplace_bundle"
ZIP_PATH = ROOT_DIR / "demo_artifacts" / "project_aegis_sre_marketplace_bundle.zip"

INCLUDE_DIRS = ("aegis_agents", "control_plane", "domain", "evals")
INCLUDE_ROOT_FILES = ("requirements.txt", ".env.example", "Dockerfile", "README.md")

SKIP_NAMES = {"__pycache__", ".pytest_cache", ".venv", ".git", ".env"}
SKIP_SUFFIXES = (".pyc", ".pyo", ".bak", ".log")

ANONYMIZATION_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"C:\\Users\\[^\\/\s]+", re.IGNORECASE), "<USER_HOME>"),
    (re.compile(r"/Users/[^/\s]+"), "<USER_HOME>"),
    (re.compile(r"\bMERCH-[A-Z0-9-]+\b"), "MERCHANT-ANON-01"),
    (re.compile(r"\bGLOBAL-TREASURY-[0-9]+\b"), "CLIENT-TREASURY-ANON"),
    (re.compile(r"\bDE89370400440532013000\b"), "ANON000000000000000000"),
    (re.compile(r"\bGB29NWBK60161331926819\b"), "ANON000000000000000001"),
    (re.compile(r"\bNexusPay\b"), "EnterpriseSettlementGateway"),
]


def _should_skip(path: Path) -> bool:
    if any(part in SKIP_NAMES for part in path.parts):
        return True
    if path.name.endswith(SKIP_SUFFIXES):
        return True
    return False


def _sanitize_text(content: str) -> str:
    sanitized = content
    for pattern, replacement in ANONYMIZATION_RULES:
        sanitized = pattern.sub(replacement, sanitized)
    return sanitized


def _copy_sanitized_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    text_extensions = {".py", ".md", ".txt", ".html", ".json", ".toml", ".yaml", ".yml", ".example"}
    if src.suffix.lower() in text_extensions or src.name in {"Dockerfile", ".env.example"}:
        try:
            raw = src.read_text(encoding="utf-8")
            dst.write_text(_sanitize_text(raw), encoding="utf-8")
            return
        except UnicodeDecodeError:
            pass
    shutil.copy2(src, dst)


def _iter_files(base: Path) -> Iterable[Path]:
    for root, dirs, files in os.walk(base):
        root_path = Path(root)
        dirs[:] = [d for d in dirs if not _should_skip(root_path / d)]
        for fname in files:
            fpath = root_path / fname
            if not _should_skip(fpath):
                yield fpath


def _write_adapter_guide(target_dir: Path) -> None:
    guide = """# Reusable AI Factory Adapter Guide (Anonymized Marketplace Artifact)

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
"""
    (target_dir / "ADAPTER_GUIDE.md").write_text(guide, encoding="utf-8")


def export_bundle() -> dict:
    if BUNDLE_DIR.exists():
        shutil.rmtree(BUNDLE_DIR)
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

    exported_count = 0
    for dirname in INCLUDE_DIRS:
        src_dir = ROOT_DIR / dirname
        if not src_dir.exists():
            continue
        for src_file in _iter_files(src_dir):
            rel = src_file.relative_to(ROOT_DIR)
            _copy_sanitized_file(src_file, BUNDLE_DIR / rel)
            exported_count += 1

    for fname in INCLUDE_ROOT_FILES:
        src_file = ROOT_DIR / fname
        if src_file.exists() and not _should_skip(src_file):
            _copy_sanitized_file(src_file, BUNDLE_DIR / fname)
            exported_count += 1

    _write_adapter_guide(BUNDLE_DIR)
    exported_count += 1

    ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        for fpath in _iter_files(BUNDLE_DIR):
            zf.write(fpath, arcname=str(fpath.relative_to(BUNDLE_DIR)))

    return {
        "bundle_dir": str(BUNDLE_DIR),
        "zip_path": str(ZIP_PATH),
        "exported_files": exported_count,
        "anonymized": True,
    }


if __name__ == "__main__":
    summary = export_bundle()
    print(f"TASK 7 OK: Exported {summary['exported_files']} anonymized files -> {summary['bundle_dir']}")
    print(f"Archive created: {summary['zip_path']}")