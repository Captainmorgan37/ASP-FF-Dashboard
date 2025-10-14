# secrets_diagnostics.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import os, json

try:
    import boto3  # vendored in /vendor
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None
    BotoCoreError = ClientError = Exception  # type: ignore

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
        return any(r.status != "OK" for r in self.rows)

def _read_env(name: str) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "", "null", "None") else None

def _sm_client():
    if not boto3:
        return None
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None
    return boto3.client("secretsmanager", region_name=region)

def _read_secret(name: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (value, error_string)."""
    sm = _sm_client()
    if not sm:
        return None, "boto3 not available"
    try:
        resp = sm.get_secret_value(SecretId=name)
        s = resp.get("SecretString") or ""
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and "value" in obj:
                return str(obj["value"]), None
        except json.JSONDecodeError:
            pass
        return (s or None), None
    except Exception as e:
        code = ""
        try:
            code = e.response.get("Error", {}).get("Code", "")  # type: ignore[attr-defined]
        except Exception:
            pass
        msg = f"{e.__class__.__name__}" + (f": {code}" if code else "")
        return None, msg

def _row(label: str, env_key: str, secret_key: Optional[str]) -> SecretRow:
    env_v = _read_env(env_key)
    if env_v:
        return SecretRow(label, "OK", f"ENV:{env_key}", "set")
    if secret_key:
        val, err = _read_secret(secret_key)
        if val is not None:
            return SecretRow(label, "OK", f"SM:{secret_key}", "set")
        return SecretRow(label, "MISSING", f"SM:{secret_key}", err or "not found")
    return SecretRow(label, "MISSING", "-", "not found")

def collect_secret_diagnostics() -> List[SecretSection]:
    rows = [
        _row("FL3XX token",     "FL3XX_TOKEN",     "FL3XX_TOKEN"),
        _row("FL3XX base URL",  "FL3XX_BASE_URL",  "FL3XX_BASE_URL"),
        _row("Admin token",     "ADMIN_TOKEN",     "ADMIN_TOKEN"),
        _row("Self base URL",   "SELF_BASE_URL",   "SELF_BASE_URL"),
    ]
    # Always return one section so the UI never shows an empty card
    return [SecretSection(title="Core integrations", rows=rows)]
