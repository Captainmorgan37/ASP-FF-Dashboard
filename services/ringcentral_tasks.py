from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import requests

RC_BASE = "https://platform.ringcentral.com"


class RingCentralConfigError(RuntimeError):
    """Raised when required RingCentral configuration is missing."""


def _read_streamlit_secret(name: str) -> str | None:
    """Best-effort lookup from Streamlit secrets when running in Streamlit."""
    try:
        import streamlit as st

        ringcentral = st.secrets.get("ringcentral", {})
        value = ringcentral.get(name)
        return str(value) if value else None
    except Exception:
        return None


def _read_config(name: str) -> str:
    env_name = f"RINGCENTRAL_{name.upper()}"
    value = os.getenv(env_name) or _read_streamlit_secret(name)
    if not value:
        raise RingCentralConfigError(
            f"Missing RingCentral setting '{name}'. Configure {env_name} or streamlit secrets.ringcentral.{name}."
        )
    return value


def get_token() -> str:
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": _read_config("jwt"),
    }

    response = requests.post(
        f"{RC_BASE}/restapi/oauth/token",
        data=data,
        auth=(_read_config("client_id"), _read_config("client_secret")),
        timeout=20,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _team_messaging_post(endpoint: str, payload: dict[str, object]) -> dict[str, object]:
    token = get_token()
    response = requests.post(
        f"{RC_BASE}{endpoint}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def create_task(subject: str, description: str, hours_until_due: int = 4) -> dict[str, object]:
    due = (datetime.now(timezone.utc) + timedelta(hours=hours_until_due)).isoformat().replace("+00:00", "Z")
    payload = {
        "subject": subject,
        "description": description,
        "dueDate": due,
    }
    return _team_messaging_post(
        f"/team-messaging/v1/chats/{_read_config('team_id')}/tasks",
        payload,
    )


def create_note(text: str) -> dict[str, object]:
    payload = {"text": text}
    return _team_messaging_post(
        f"/team-messaging/v1/chats/{_read_config('team_id')}/posts",
        payload,
    )
