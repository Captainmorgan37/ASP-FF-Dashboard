"""Streamlit entrypoint without spaces for AWS App Runner."""

from __future__ import annotations

import runpy
import os
from pathlib import Path
from typing import Iterable


def _candidate_script_paths(base: Path) -> Iterable[Path]:
    """Yield possible dashboard script paths in order of preference."""

    yield base.with_name("ASP FF Dashboard.py")
    yield base.with_name("asp_ff_dashboard.py")
    yield base.with_name("asp ff dashboard.py")


def _find_dashboard_script(start: Path) -> Path:
    for candidate in _candidate_script_paths(start):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Expected Streamlit dashboard script next to app.py (tried: 'ASP FF "
        "Dashboard.py', 'asp_ff_dashboard.py', 'asp ff dashboard.py')."
    )


os.environ.setdefault("STREAMLIT_SERVER_ENABLE_WEBSOCKET_COMPRESSION", "false")

SCRIPT_PATH = _find_dashboard_script(Path(__file__))

runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
