"""Streamlit entrypoint without spaces for AWS App Runner."""

import runpy
from pathlib import Path

SCRIPT_PATH = Path(__file__).with_name("ASP FF Dashboard.py")

if not SCRIPT_PATH.exists():
    raise FileNotFoundError(
        f"Expected Streamlit dashboard script at {SCRIPT_PATH}. "
        "Ensure the source archive includes 'ASP FF Dashboard.py'."
    )

runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
