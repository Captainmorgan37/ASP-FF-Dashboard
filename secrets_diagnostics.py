diff --git a//dev/null b/secrets_diagnostics.py
index 0000000000000000000000000000000000000000..2eb74cc2b1a16e80107fdc993cd98e23cb3cf38c 100644
--- a//dev/null
+++ b/secrets_diagnostics.py
@@ -0,0 +1,319 @@
+"""Helpers to inspect secret availability for the NiceGUI dashboard."""
+from __future__ import annotations
+
+from dataclasses import dataclass
+import json
+import os
+from pathlib import Path
+from typing import Any, Mapping
+
+try:
+    import tomllib  # Python 3.11+
+except ModuleNotFoundError:  # pragma: no cover - Python <3.11 compatibility
+    import tomli as tomllib  # type: ignore
+
+
+@dataclass
+class SecretCheck:
+    """Represents a single secret diagnostic entry."""
+
+    item: str
+    status: str
+    source: str
+    detail: str
+
+
+@dataclass
+class SecretSection:
+    """A collection of checks for one integration."""
+
+    title: str
+    rows: list[SecretCheck]
+
+    @property
+    def has_warning(self) -> bool:
+        return any(row.status not in {"OK", "Info"} for row in self.rows)
+
+
+class SecretStore:
+    """Resolve secrets from environment variables or structured files."""
+
+    def __init__(self) -> None:
+        self._env: Mapping[str, str] = dict(os.environ)
+        self._structured, self._structured_source = _load_structured_secrets()
+
+    def resolve(
+        self,
+        *keys: str,
+        allow_blank: bool = False,
+        default: Any | None = None,
+    ) -> tuple[Any | None, str | None]:
+        for key in keys:
+            value, source = self._lookup(key)
+            value = _normalize_secret_value(value, allow_blank=allow_blank)
+            if value is not None:
+                return value, source
+        return default, None
+
+    def _lookup(self, key: str) -> tuple[Any | None, str | None]:
+        if "." in key:
+            return self._lookup_structured(key)
+
+        env_value = self._env.get(key)
+        if env_value is not None:
+            return env_value, f"env:{key}"
+
+        return self._lookup_structured(key)
+
+    def _lookup_structured(self, dotted_key: str) -> tuple[Any | None, str | None]:
+        if not self._structured:
+            return None, None
+
+        parts = dotted_key.split(".")
+        current: Any = self._structured
+        for part in parts:
+            if not isinstance(current, Mapping):
+                return None, None
+            current = _mapping_get(current, part)
+            if current is None:
+                return None, None
+
+        return current, self._structured_source
+
+
+def _normalize_secret_value(value: Any, *, allow_blank: bool = False) -> Any | None:
+    if value is None:
+        return None
+
+    if isinstance(value, str):
+        trimmed = value.strip()
+        if not trimmed and not allow_blank:
+            return None
+        return trimmed
+
+    if isinstance(value, (list, tuple, set)):
+        seq = [item for item in value if item is not None]
+        if seq or allow_blank:
+            return seq
+        return None
+
+    if isinstance(value, (int, float, bool)):
+        return value
+
+    if isinstance(value, Mapping):
+        return value if value or allow_blank else None
+
+    return value
+
+
+def _mapping_get(mapping: Mapping[str, Any], key: str) -> Any | None:
+    target = key.lower().replace("-", "_")
+    for existing_key, value in mapping.items():
+        if existing_key == key:
+            return value
+        if isinstance(existing_key, str):
+            normalized = existing_key.lower().replace("-", "_")
+            if normalized == target:
+                return value
+    return None
+
+
+def _load_structured_secrets() -> tuple[Mapping[str, Any] | None, str | None]:
+    """Load a structured secrets mapping from common App Runner locations."""
+
+    candidates: list[tuple[str, str]] = []
+
+    env_path = os.getenv("STREAMLIT_SECRETS_PATH")
+    if env_path:
+        candidates.append((env_path, f"file:{env_path}"))
+
+    candidates.append(("/runtimes/secrets/streamlit_secrets", "file:/runtimes/secrets/streamlit_secrets"))
+
+    env_blob = os.getenv("STREAMLIT_SECRETS")
+    if env_blob:
+        candidates.append((env_blob, "env:STREAMLIT_SECRETS"))
+
+    for payload, source in candidates:
+        mapping = _parse_structured_payload(payload, source)
+        if mapping is not None:
+            return mapping, source
+
+    return None, None
+
+
+def _parse_structured_payload(payload: str, source: str) -> Mapping[str, Any] | None:
+    path = Path(payload)
+    if path.exists():
+        try:
+            text = path.read_text(encoding="utf-8")
+        except OSError:
+            return None
+        return _loads_structured_text(text)
+
+    if source.startswith("env:"):
+        return _loads_structured_text(payload)
+
+    return None
+
+
+def _loads_structured_text(text: str) -> Mapping[str, Any] | None:
+    parsers = ("toml", "json")
+    for parser in parsers:
+        try:
+            if parser == "toml":
+                return tomllib.loads(text)
+            if parser == "json":
+                return json.loads(text)
+        except Exception:
+            continue
+    return None
+
+
+def collect_secret_diagnostics(store: SecretStore | None = None) -> list[SecretSection]:
+    store = store or SecretStore()
+
+    sections = [
+        SecretSection("FL3XX API", _fl3xx_rows(store)),
+        SecretSection("FlightAware webhook (DynamoDB)", _flightaware_rows(store)),
+    ]
+
+    return [section for section in sections if section.rows]
+
+
+def _fl3xx_rows(store: SecretStore) -> list[SecretCheck]:
+    rows: list[SecretCheck] = []
+
+    token, token_source = store.resolve("FL3XX_API_TOKEN", "fl3xx_api.api_token", allow_blank=True)
+    if token:
+        rows.append(
+            SecretCheck(
+                item="API token",
+                status="OK",
+                source=token_source or "unknown",
+                detail="FL3XX API token detected.",
+            )
+        )
+    else:
+        rows.append(
+            SecretCheck(
+                item="API token",
+                status="Missing",
+                source="",
+                detail="Define FL3XX_API_TOKEN or [fl3xx_api].api_token in streamlit_secrets.",
+            )
+        )
+
+    header_name, header_name_source = store.resolve(
+        "FL3XX_AUTH_HEADER_NAME",
+        "fl3xx_api.auth_header_name",
+        allow_blank=True,
+    )
+    if header_name:
+        rows.append(
+            SecretCheck(
+                item="Auth header name",
+                status="OK",
+                source=header_name_source or "unknown",
+                detail=f"Using custom header '{header_name}'.",
+            )
+        )
+    else:
+        rows.append(
+            SecretCheck(
+                item="Auth header name",
+                status="Info",
+                source="",
+                detail="Default header will be used (X-AUTH-TOKEN).",
+            )
+        )
+
+    auth_header, auth_header_source = store.resolve(
+        "fl3xx_api.auth_header",
+        allow_blank=True,
+    )
+    if auth_header:
+        rows.append(
+            SecretCheck(
+                item="Auth header value",
+                status="OK",
+                source=auth_header_source or "unknown",
+                detail="Custom auth header detected.",
+            )
+        )
+
+    return rows
+
+
+def _flightaware_rows(store: SecretStore) -> list[SecretCheck]:
+    rows: list[SecretCheck] = []
+
+    region, region_source = store.resolve("AWS_REGION", allow_blank=True)
+    access_key, access_source = store.resolve("AWS_ACCESS_KEY_ID", allow_blank=True)
+    secret_key, secret_source = store.resolve("AWS_SECRET_ACCESS_KEY", allow_blank=True)
+
+    if region and access_key and secret_key:
+        rows.append(
+            SecretCheck(
+                item="AWS credentials",
+                status="OK",
+                source=", ".join(filter(None, {region_source, access_source, secret_source})),
+                detail="Region and credential keys detected.",
+            )
+        )
+    else:
+        missing: list[str] = []
+        if not region:
+            missing.append("AWS_REGION")
+        if not access_key:
+            missing.append("AWS_ACCESS_KEY_ID")
+        if not secret_key:
+            missing.append("AWS_SECRET_ACCESS_KEY")
+        rows.append(
+            SecretCheck(
+                item="AWS credentials",
+                status="Missing",
+                source="",
+                detail="Missing secrets: " + ", ".join(missing) if missing else "Provide AWS credentials.",
+            )
+        )
+
+    session_token, session_source = store.resolve("AWS_SESSION_TOKEN", allow_blank=True)
+    if session_token:
+        rows.append(
+            SecretCheck(
+                item="AWS session token",
+                status="OK",
+                source=session_source or "unknown",
+                detail="Session token detected.",
+            )
+        )
+
+    table_name, table_source = store.resolve("FLIGHTAWARE_ALERTS_TABLE", allow_blank=True)
+    if table_name:
+        rows.append(
+            SecretCheck(
+                item="DynamoDB table",
+                status="OK",
+                source=table_source or "unknown",
+                detail=f"Using table '{table_name}'.",
+            )
+        )
+    else:
+        rows.append(
+            SecretCheck(
+                item="DynamoDB table",
+                status="Info",
+                source="",
+                detail="Default table 'fa-oooi-alerts' will be used if available.",
+            )
+        )
+
+    return rows
+
+
+__all__ = [
+    "SecretCheck",
+    "SecretSection",
+    "SecretStore",
+    "collect_secret_diagnostics",
+]
