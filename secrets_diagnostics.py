# secrets_diagnostics.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterable, List, Optional
import os
import json

try:
    import boto3  # optional at runtime; handled if missing
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:  # if vendor/ or deps not present yet
    boto3 = None
    BotoCoreError = ClientError = Exception

@dataclass
class SecretRow:
    item: str
    status: str
    source: str
    detail: str

@dataclass
class SecretSection:
    title: str
    rows: List[SecretRow] = field(default_factory=list)

    @property
    def has_warning(self) -> bool:
        # expand by default if any row isn’t “OK”
        return any(r.status != "OK" for r in self.rows)

def _read_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "", "null", "None") else None

def _read_secret(name: str) -> Optional[str]:
    if not boto3:
        return None
    try:
        sm = boto3.client("secretsmanager")
        val = sm.get_secret_value(SecretId=name)
        s = val.get("SecretString") or ""
        # support JSON-style secrets; prefer a “value” key if present
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "value" in obj:
                return str(obj["value"])
        except json.JSONDecodeError:
            pass
        return s or None
    except (BotoCoreError, ClientError, Exception):
        return None

def _coerce_keys(keys: str | Iterable[str] | None) -> tuple[str, ...]:
    if not keys:
        return ()
    if isinstance(keys, str):
        return (keys,)
    return tuple(keys)


def _row(
    label: str,
    env_keys: str | Iterable[str],
    secret_keys: Optional[str | Iterable[str]] = None,
) -> SecretRow:
    for key in _coerce_keys(env_keys):
        env_v = _read_env(key)
        if env_v:
            return SecretRow(item=label, status="OK", source=f"ENV:{key}", detail="set")

    for key in _coerce_keys(secret_keys):
        sec_v = _read_secret(key)
        if sec_v:
            return SecretRow(item=label, status="OK", source=f"SM:{key}", detail="set")

    checked = [f"ENV:{key}" for key in _coerce_keys(env_keys)]
    checked.extend(f"SM:{key}" for key in _coerce_keys(secret_keys))
    detail = "not found"
    if checked:
        detail = f"not found ({', '.join(checked)})"
    return SecretRow(item=label, status="MISSING", source="-", detail=detail)

def collect_secret_diagnostics() -> List[SecretSection]:
    rows = [
        _row(
            "FL3XX token",
            ("FL3XX_TOKEN", "FL3XX_API_TOKEN"),
            ("FL3XX_TOKEN", "FL3XX_API_TOKEN"),
        ),
        _row("FL3XX base URL", "FL3XX_BASE_URL", "FL3XX_BASE_URL"),
        _row("Admin token", "ADMIN_TOKEN", "ADMIN_TOKEN"),
        _row("Self base URL", "SELF_BASE_URL", "SELF_BASE_URL"),
    ]
    return [SecretSection(title="Core integrations", rows=rows)]
