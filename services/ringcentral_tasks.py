import requests
from datetime import datetime, timedelta, timezone
import streamlit as st

RC_BASE = "https://platform.ringcentral.com"


def get_token():
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": st.secrets["ringcentral"]["jwt"],
    }

    r = requests.post(
        f"{RC_BASE}/restapi/oauth/token",
        data=data,
        auth=(
            st.secrets["ringcentral"]["client_id"],
            st.secrets["ringcentral"]["client_secret"],
        ),
    )

    r.raise_for_status()
    return r.json()["access_token"]


def create_task(subject, description, hours_until_due=4):
    token = get_token()

    due = (
        datetime.now(timezone.utc) + timedelta(hours=hours_until_due)
    ).isoformat().replace("+00:00", "Z")

    payload = {
        "subject": subject,
        "description": description,
        "dueDate": due,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    r = requests.post(
        f"{RC_BASE}/team-messaging/v1/chats/{st.secrets['ringcentral']['team_id']}/tasks",
        headers=headers,
        json=payload,
    )

    r.raise_for_status()
    return r.json()
