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

def _read_secret(name: str) -> tuple[Optional[str], Optional[str]]:
    """Return (value, error)."""
    if not boto3:
        return None, "boto3 not available"
    try:
        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None
        sm = boto3.client("secretsmanager", region_name=region)
        val = sm.get_secret_value(SecretId=name)
        s = val.get("SecretString") or ""
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "value" in obj:
                return str(obj["value"]), None
        except json.JSONDecodeError:
            pass
        return (s or None), None
    except Exception as e:
        # include class name and short message
        return None, f"{e.__class__.__name__}: {getattr(e, 'response', {}).get('Error', {}).get('Code', '') or str(e)}"

def _row(label: str, env_key: str, secret_key: Optional[str] = None) -> SecretRow:
    env_v = _read_env(env_key)
    if env_v:
        return SecretRow(item=label, status="OK", source=f"ENV:{env_key}", detail="set")
    if secret_key:
        val, err = _read_secret(secret_key)
        if val is not None:
            return SecretRow(item=label, status="OK", source=f"SM:{secret_key}", detail="set")
        return SecretRow(item=label, status="MISSING", source=f"SM:{secret_key}", detail=err or "not found")
    return SecretRow(item=label, status="MISSING", source="-", detail="not found")

def _coerce_keys(keys: str | Iterable[str] | None) -> tuple[str, ...]:
    if not keys:
        return ()
    if isinstance(keys, str):
        return (keys,)
    return tuple(keys)



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
