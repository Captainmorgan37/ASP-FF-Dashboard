import ast
from collections.abc import Mapping
from pathlib import Path

import streamlit as st


MODULE_PATH = Path(__file__).resolve().parents[1] / "ASP FF Dashboard.py"

with MODULE_PATH.open("r", encoding="utf-8") as fp:
    MODULE_SOURCE = fp.read()

MODULE_AST = ast.parse(MODULE_SOURCE, filename=str(MODULE_PATH))

_TARGET_FUNC = None
for node in MODULE_AST.body:
    if isinstance(node, ast.FunctionDef) and node.name == "_build_fl3xx_config_from_secrets":
        _TARGET_FUNC = node
        break

if _TARGET_FUNC is None:  # pragma: no cover - safety
    raise RuntimeError("_build_fl3xx_config_from_secrets not found in dashboard module")

FUNCTION_SOURCE = ast.get_source_segment(MODULE_SOURCE, _TARGET_FUNC)

assert FUNCTION_SOURCE is not None

_namespace = {"st": st}
exec(
    "from collections.abc import Mapping\n"
    "from typing import Any\n"
    "import os\n"
    "from fl3xx_client import DEFAULT_FL3XX_BASE_URL, Fl3xxApiConfig\n"
    f"{FUNCTION_SOURCE}\n",
    _namespace,
)

_build_fl3xx_config_from_secrets = _namespace["_build_fl3xx_config_from_secrets"]


class CustomMapping(Mapping):
    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def get(self, key, default=None):  # pragma: no cover - delegated to dict
        return self._data.get(key, default)


def test_build_config_accepts_mapping(monkeypatch):
    secrets_mapping = {
        "fl3xx_api": CustomMapping(
            {
                "api_token": "token-123",
                "auth_header": "Bearer abc",
            }
        )
    }

    monkeypatch.setattr(st, "secrets", secrets_mapping, raising=False)

    config = _build_fl3xx_config_from_secrets()

    assert config.api_token == "token-123"
    assert config.auth_header == "Bearer abc"
