"""Repository-level tests.

    cd tests && uv run pytest                    # static checks only (no stack needed)
    cd tests && uv run pytest -m integration     # against the running docker compose stack

Docstrings hold objective / steps / expected; `actual(...)` records the observed result. Both go to
tests/reports/*.xml, from which make_report.py builds the test-case table.
"""
from __future__ import annotations

import inspect
import json
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _document(request, record_property):
    record_property("doc", inspect.getdoc(request.function) or "")


@pytest.fixture
def actual(record_property):
    def record(text) -> None:
        if isinstance(text, dict):
            text = {str(k): v for k, v in text.items()}
        text = text if isinstance(text, str) else json.dumps(text, default=str)
        print(text)
        record_property("actual", text)
    return record


def compose(*args: str, stdin: str | None = None, timeout: float = 600, check: bool = True) -> str:
    """Run `docker compose <args>` in the repository root and return stdout."""
    proc = subprocess.run(["docker", "compose", *args], cwd=REPO, input=stdin, text=True,
                          capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}")
    return proc.stdout


def pytest_collection_modifyitems(config, items):
    """Integration tests need the stack: run them only with `-m integration`."""
    if "integration" in (config.getoption("-m") or ""):
        return
    skip = pytest.mark.skip(reason="needs the running stack: pytest -m integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
