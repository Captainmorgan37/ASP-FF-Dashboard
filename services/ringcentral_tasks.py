from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

DEFAULT_RC_BASE = "https://platform.ringcentral.com"


class RingCentralConfigError(RuntimeError):
    """Raised when required RingCentral configuration is missing."""


class RingCentralApiError(RuntimeError):
    """Raised when RingCentral returns an API error response."""


def _read_streamlit_secret(name: str) -> str | None:
    """Best-effort lookup from Streamlit secrets when running in Streamlit."""
    try:
        import streamlit as st

        ringcentral = st.secrets.get("ringcentral", {})
        value = ringcentral.get(name)
        return str(value).strip() if value else None
    except Exception:
        return None


def _read_optional_config(name: str) -> str | None:
    env_name = f"RINGCENTRAL_{name.upper()}"
    value = os.getenv(env_name) or _read_streamlit_secret(name)
    if value is None:
        return None
    trimmed = str(value).strip()
    return trimmed if trimmed else None


def _read_config(name: str) -> str:
    value = _read_optional_config(name)
    if not value:
        env_name = f"RINGCENTRAL_{name.upper()}"
        raise RingCentralConfigError(
            f"Missing RingCentral setting '{name}'. Configure {env_name} or streamlit secrets.ringcentral.{name}."
        )
    return value


def _rc_base() -> str:
    return (_read_optional_config("server_url") or DEFAULT_RC_BASE).rstrip("/")


def _extract_error_message(response: requests.Response) -> str:
    try:
        payload: Any = response.json()
    except Exception:
        text = (response.text or "").strip()
        return text or f"HTTP {response.status_code}"

    if isinstance(payload, dict):
        message = payload.get("message") or payload.get("error_description") or payload.get("error")
        code = payload.get("errorCode") or payload.get("error")
        if message and code:
            return f"{code}: {message}"
        if message:
            return str(message)
    return str(payload)


def _raise_for_status(response: requests.Response, context: str) -> None:
    if response.ok:
        return
    message = _extract_error_message(response)
    raise RingCentralApiError(f"{context} failed ({response.status_code}): {message}")


def get_token() -> str:
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": _read_config("jwt"),
    }

    response = requests.post(
        f"{_rc_base()}/restapi/oauth/token",
        data=data,
        auth=(_read_config("client_id"), _read_config("client_secret")),
        timeout=20,
    )
    if not response.ok:
        message = _extract_error_message(response)
        raise RingCentralConfigError(
            "Unable to obtain RingCentral token. "
            f"{message}. Check client_id/client_secret/jwt and ringcentral.server_url for sandbox vs production."
        )
    return response.json()["access_token"]


def _chat_or_team_id() -> str:
    return _read_optional_config("chat_id") or _read_config("team_id")


def _team_messaging_post(endpoint: str, payload: dict[str, object]) -> dict[str, object]:
    token = get_token()
    response = requests.post(
        f"{_rc_base()}{endpoint}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )
    _raise_for_status(response, "RingCentral request")
    return response.json()


def _post_with_fallback(endpoints: list[str], payload: dict[str, object]) -> dict[str, object]:
    last_error: RingCentralApiError | None = None
    for endpoint in endpoints:
        try:
            return _team_messaging_post(endpoint, payload)
        except RingCentralApiError as exc:
            last_error = exc
            if "(404)" not in str(exc):
                raise
    if last_error is not None:
        raise last_error
    raise RingCentralApiError("RingCentral request failed: no endpoint configured")


def create_task(subject: str, description: str, hours_until_due: int = 4) -> dict[str, object]:
    due = (datetime.now(timezone.utc) + timedelta(hours=hours_until_due)).isoformat().replace("+00:00", "Z")
    payload = {
        "subject": subject,
        "description": description,
        "dueDate": due,
    }
    target_id = _chat_or_team_id()
    return _post_with_fallback(
        [
            f"/team-messaging/v1/chats/{target_id}/tasks",
            f"/team-messaging/v1/teams/{target_id}/tasks",
        ],
        payload,
    )


def create_note(text: str) -> dict[str, object]:
    payload = {"text": text}
    target_id = _chat_or_team_id()
    return _post_with_fallback(
        [
            f"/team-messaging/v1/chats/{target_id}/posts",
            f"/team-messaging/v1/teams/{target_id}/posts",
        ],
        payload,
    )


def get_diagnostics() -> dict[str, object]:
    """Return non-secret diagnostic details for troubleshooting config/path issues."""
    base_url = _rc_base()
    target_id = _chat_or_team_id()
    mode = "chat_id" if _read_optional_config("chat_id") else "team_id"

    return {
        "base_url": base_url,
        "target_mode": mode,
        "target_id_suffix": target_id[-6:] if len(target_id) > 6 else target_id,
        "configured": {
            "client_id": bool(_read_optional_config("client_id")),
            "client_secret": bool(_read_optional_config("client_secret")),
            "jwt": bool(_read_optional_config("jwt")),
            "chat_id": bool(_read_optional_config("chat_id")),
            "team_id": bool(_read_optional_config("team_id")),
            "server_url": bool(_read_optional_config("server_url")),
        },
        "note_endpoints": [
            f"{base_url}/team-messaging/v1/chats/{target_id}/posts",
            f"{base_url}/team-messaging/v1/teams/{target_id}/posts",
        ],
        "task_endpoints": [
            f"{base_url}/team-messaging/v1/chats/{target_id}/tasks",
            f"{base_url}/team-messaging/v1/teams/{target_id}/tasks",
        ],
        "token_endpoint": f"{base_url}/restapi/oauth/token",
    }
