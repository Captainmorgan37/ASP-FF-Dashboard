"""Shim module to launch the Streamlit dashboard with a filesystem-friendly name."""

from __future__ import annotations

import runpy
from pathlib import Path

_SCRIPT_PATH = Path(__file__).with_name("ASP FF Dashboard.py")

if not _SCRIPT_PATH.exists():
    raise FileNotFoundError(
        f"Expected Streamlit dashboard script at {_SCRIPT_PATH}. "
        "Ensure the deployment package includes 'ASP FF Dashboard.py'."
    )

runpy.run_path(str(_SCRIPT_PATH), run_name="__main__")
