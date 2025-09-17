# daily_ops_dashboard.py
import re
import sqlite3
import imaplib, email
from io import BytesIO
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from dateutil import parser as dateparse

# ============================
# Page config
# ============================
st.set_page_config(page_title="Daily Ops Dashboard (Schedule + Status)", layout="wide")
st.title("Daily Ops Dashboard (Schedule + Status)")
st.caption(
    "Times shown in **UTC**. Some airports may be blank (non-ICAO). "
    "Rows with non-tail placeholders (e.g., “Remove OCS”, “Add EMB”) are hidden."
)

# ============================
# Auto-refresh controls (no page reload fallback)
# ============================
ar1, ar2 = st.columns([1, 1])
with ar1:
    auto_refresh = st.checkbox("Auto-refresh", value=True, help="Re-run the app so countdowns update.")
with ar2:
    refresh_sec = st.number_input("Refresh every (sec)", min_value=5, max_value=120, value=30, step=5)

if auto_refresh:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=int(refresh_sec * 1000), key="ops_auto_refresh")
    except Exception:
        st.warning(
            "Auto-refresh requires the 'streamlit-autorefresh' package. "
            "Add `streamlit-autorefresh` to requirements.txt (or disable Auto-refresh)."
        )

# ============================
# SQLite persistence (statuses + CSV)
# ============================
DB_PATH = "status_store.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS status_events (
            booking TEXT NOT NULL,
            event_type TEXT NOT NULL,  -- 'Departure' | 'Arrival' | 'ArrivalForecast' | 'Diversion'
            status TEXT NOT NULL,      -- '🟢 DEPARTED', '🟣 ARRIVED', '🟠 LATE ARRIVAL', '🔴 DELAY', '🟦 ARRIVING SOON', '🔷 DIVERTED to ___'
            actual_time_utc TEXT,
            delta_min INTEGER,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (booking, event_type)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS csv_store (
            id INTEGER PRIMARY KEY CHECK (id=1),
            name TEXT,
            content BLOB,
            uploaded_at TEXT NOT NULL
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS email_cursor (
            mailbox TEXT PRIMARY KEY,
            last_uid INTEGER
        )
        """)

def load_status_map() -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT booking, event_type, status, actual_time_utc, delta_min
            FROM status_events
        """).fetchall()
    m = {}
    for booking, event_type, status, actual, delta in rows:
        m.setdefault(booking, {})[event_type] = {
            "status": status, "actual_time_utc": actual, "delta_min": delta
        }
    return m

def upsert_status(booking, event_type, status, actual_time_iso, delta_min):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO status_events (booking, event_type, status, actual_time_utc, delta_min, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(booking, event_type) DO UPDATE SET
                status=excluded.status,
                actual_time_utc=excluded.actual_time_utc,
                delta_min=excluded.delta_min,
                updated_at=datetime('now')
        """, (booking, event_type, status, actual_time_iso,
              int(delta_min) if delta_min is not None else None))

def save_csv_to_db(name: str, content_bytes: bytes):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO csv_store (id, name, content, uploaded_at)
            VALUES (1, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                content=excluded.content,
                uploaded_at=datetime('now')
        """, (name, content_bytes))

def load_csv_from_db():
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT name, content, uploaded_at FROM csv_store WHERE id=1").fetchone()
    if row:
        return row[0], row[1], row[2]
    return None, None, None

def get_last_uid(mailbox: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT last_uid FROM email_cursor WHERE mailbox=?", (mailbox,)).fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def set_last_uid(mailbox: str, uid: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        INSERT INTO email_cursor (mailbox, last_uid)
        VALUES (?, ?)
        ON CONFLICT(mailbox) DO UPDATE SET last_uid=excluded.last_uid
        """, (mailbox, int(uid)))

init_db()

# ============================
# Helpers
# ============================
FAKE_TAIL_PATTERNS = [
    re.compile(r"^\s*(add|remove)\b", re.I),
    re.compile(r"\b(ocs|emb)\b", re.I),
]

def is_real_tail(tail: str) -> bool:
    if not isinstance(tail, str) or not tail.strip():
        return False
    for pat in FAKE_TAIL_PATTERNS:
        if pat.search(tail):
            return False
    return True

def parse_utc_ddmmyyyy_hhmmz(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace("Z", "z", regex=False).str.replace("z", "", regex=False)
    return pd.to_datetime(s, format="%d.%m.%Y %H:%M", errors="coerce", utc=True)

def fmt_td(td):
    if td is None or pd.isna(td):
        return "—"
    if isinstance(td, pd.Timedelta):
        td = td.to_pytimedelta()
    sign = "-" if td.total_seconds() < 0 else ""
    td_abs = abs(td)
    hours, remainder = divmod(int(td_abs.total_seconds()), 3600)
    minutes = remainder // 60
    return f"{sign}{hours:02d}:{minutes:02d}"

def flight_time_hhmm_from_decimal(hours_decimal) -> str:
    try:
        mins = int(round(float(hours_decimal) * 60))
        return f"{mins // 60:02d}:{mins % 60:02d}"
    except Exception:
        return "—"

def classify_account(account_val: str) -> str:
    if isinstance(account_val, str) and "airsprint inc" in account_val.lower():
        return "OCS"
    return "Owner"

def type_badge(flight_type: str) -> str:
    return {"OCS": "🟢 OCS", "Owner": "🔵 Owner"}.get(flight_type, "⚪︎")

def default_status(dep_utc: pd.Timestamp, eta_utc: pd.Timestamp) -> str:
    now = datetime.now(timezone.utc)
    if pd.notna(dep_utc) and now < dep_utc:
        return "🟡 SCHEDULED"
    if pd.notna(dep_utc) and now >= dep_utc and (pd.isna(eta_utc) or now < eta_utc):
        return "🟢 DEPARTED"
    if pd.notna(eta_utc) and now >= eta_utc:
        return "🟣 ARRIVED"
    return "🟡 SCHEDULED"

def utc_datetime_picker(label: str, default_dt_utc: datetime) -> datetime:
    # (left in place; unused now that simulator is removed)
    d = st.date_input(f"{label} — Date (UTC)", value=default_dt_utc.date(), key=f"{label}-date")
    t = st.time_input(f"{label} — Time (UTC)", value=default_dt_utc.time().replace(microsecond=0), key=f"{label}-time")
    return datetime.combine(d, t).replace(tzinfo=timezone.utc)

# ---------- Subject-aware parsing (named groups for reliability) ----------
SUBJ_TAIL_RE = re.compile(r"\bC-[A-Z0-9]{4}\b")
SUBJ_CALLSIGN_RE = re.compile(r"\bASP\d{3,4}\b")

SUBJ_PATTERNS = {
    "Arrival": re.compile(
        r"\barrived\b.*\bat\s+(?P<at>[A-Z]{3,4})\b(?:.*\bfrom\s+(?P<from>[A-Z]{3,4})\b)?",
        re.I,
    ),
    "ArrivalForecast": re.compile(
        r"\b(?:expected to arrive|arriving soon)\b.*\bat\s+(?P<at>[A-Z]{3,4})\b.*\bin\s+(?P<mins>\d+)\s*(?:min|mins|minutes)\b",
        re.I,
    ),
    "Departure": re.compile(
        r"\b(?:has\s+)?departed\b.*?(?:from\s+)?(?P<from>[A-Z]{3,4})\b(?:.*?\b(?:for|to)\s+(?P<to>[A-Z]{3,4})\b)?",
        re.I,
    ),
    "Diversion": re.compile(
        r"\bdiverted to\s+(?P<to>[A-Z]{3,4})\b",
        re.I,
    ),
}

def parse_subject_line(subject: str, now_utc: datetime):
    if not subject:
        return {"event_type": None}
    tail_m = SUBJ_TAIL_RE.search(subject)
    tail = tail_m.group(0) if tail_m else None
    callsign_m = SUBJ_CALLSIGN_RE.search(subject)
    callsign = callsign_m.group(0) if callsign_m else None

    result = {"event_type": None, "tail": tail, "callsign": callsign,
              "at_airport": None, "from_airport": None, "to_airport": None,
              "minutes_until": None, "actual_time_utc": None}

    m = SUBJ_PATTERNS["Arrival"].search(subject)
    if m:
        result["event_type"] = "Arrival"
        result["at_airport"] = m.group("at")
        result["from_airport"] = m.groupdict().get("from")
        result["actual_time_utc"] = now_utc
        return result

    m = SUBJ_PATTERNS["ArrivalForecast"].search(subject)
    if m:
        result["event_type"] = "ArrivalForecast"
        result["at_airport"] = m.group("at")
        result["minutes_until"] = int(m.group("mins"))
        result["actual_time_utc"] = now_utc + timedelta(minutes=result["minutes_until"])
        return result

    m = SUBJ_PATTERNS["Departure"].search(subject)
    if m:
        result["event_type"] = "Departure"
        result["from_airport"] = m.group("from")
        result["to_airport"] = m.groupdict().get("to")
        result["actual_time_utc"] = now_utc
        return result

    m = SUBJ_PATTERNS["Diversion"].search(subject)
    if m:
        result["event_type"] = "Diversion"
        result["to_airport"] = m.group("to")
        result["actual_time_utc"] = now_utc
        return result

    return result

def parse_any_datetime_to_utc(text: str) -> datetime | None:
    # ISO-ish
    m_iso = re.search(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(Z|[+\-]\d{2}:?\d{2})?", text)
    if m_iso:
        try:
            dt = dateparse.parse(m_iso.group(0))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    # dd.mm.yyyy HH:MMZ-like or "Sep 18, 2025 01:23 UTC"
    m2_date = re.search(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|[A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})\b", text)
    m2_time = re.search(r"\b(\d{1,2}:\d{2}(:\d{2})?)\s*(Z|UTC|[+\-]\d{2}:?\d{2}|[A-Z]{2,4})?\b", text, re.I)
    try_strings = []
    if m2_date and m2_time:
        try_strings.append(m2_date.group(0) + " " + m2_time.group(0))
    elif m2_time:
        assumed = datetime.now(timezone.utc).strftime("%Y-%m-%d ") + m2_time.group(0)
        try_strings.append(assumed)

    for s in try_strings:
        try:
            dt = dateparse.parse(s, fuzzy=True)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None

def extract_event(text: str):
    if re.search(r"\bdiverted\b", text, re.I): return "Diversion"
    if re.search(r"\barriv(?:ed|al)\b", text, re.I): return "Arrival"
    if re.search(r"\bdepart(?:ed|ure)\b", text, re.I): return "Departure"
    return None

def extract_candidates(text: str):
    bookings_all = set(re.findall(r"\b([A-Z0-9]{5})\b", text))
    valid_bookings = set(df_clean["Booking"].astype(str).unique().tolist()) if 'df_clean' in globals() else set()
    bookings = sorted([b for b in bookings_all if b in valid_bookings]) if valid_bookings else sorted(bookings_all)
    tails = sorted(set(re.findall(r"\bC-[A-Z0-9]{4}\b", text)))
    event = extract_event(text)
    return bookings, tails, event

# ============================
# Controls
# ============================
c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    show_only_upcoming = st.checkbox("Show only upcoming departures", value=True)
with c2:
    limit_next_hours = st.checkbox("Limit to next X hours", value=False)
with c3:
    next_hours = st.number_input("X hours (for filter above)", min_value=1, max_value=48, value=6)

delay_threshold_min = st.number_input("Delay threshold (minutes)", min_value=1, max_value=120, value=15)

# ============================
# File upload with persistence (session + SQLite)
# ============================
uploaded = st.file_uploader("Upload your daily flights CSV (FL3XX export)", type=["csv"], key="flights_csv")

def _load_csv_from_bytes(b: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(b))

btn_cols = st.columns([1, 3, 1])
with btn_cols[0]:
    if st.button("Reset data & clear cache"):
        for k in ("csv_bytes", "csv_name", "csv_uploaded_at", "status_updates"):
            st.session_state.pop(k, None)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM status_events")
            conn.execute("DELETE FROM csv_store")
            conn.execute("DELETE FROM email_cursor")
        st.rerun()

# Priority 1: new upload
if uploaded is not None:
    csv_bytes = uploaded.getvalue()
    st.session_state["csv_bytes"] = csv_bytes
    st.session_state["csv_name"] = uploaded.name
    st.session_state["csv_uploaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    save_csv_to_db(uploaded.name, csv_bytes)
    df_raw = _load_csv_from_bytes(csv_bytes)

# Priority 2: existing session cache
elif "csv_bytes" in st.session_state:
    df_raw = _load_csv_from_bytes(st.session_state["csv_bytes"])
    st.caption(
        f"Using cached CSV: **{st.session_state.get('csv_name','flights.csv')}** "
        f"(uploaded {st.session_state.get('csv_uploaded_at','')})"
    )

# Priority 3: DB fallback (fresh session / restart / redeploy)
else:
    name, content, uploaded_at = load_csv_from_db()
    if content is not None:
        st.session_state["csv_bytes"] = content
        st.session_state["csv_name"] = name or "flights.csv"
        st.session_state["csv_uploaded_at"] = uploaded_at or ""
        df_raw = _load_csv_from_bytes(content)
        st.caption(
            f"Loaded CSV from storage: **{st.session_state['csv_name']}** "
            f"(uploaded {st.session_state['csv_uploaded_at']})"
        )
    else:
        st.info("Upload today’s FL3XX flights CSV to begin.")
        st.stop()

# ============================
# Parse & normalize
# ============================
expected_cols = [
    "Booking", "Off-Block (Est)", "On-Block (Est)",
    "From (ICAO)", "To (ICAO)",
    "Flight time (Est)", "PIC", "SIC",
    "Account", "Aircraft", "Aircraft Type", "Workflow"
]
missing = [c for c in expected_cols if c not in df_raw.columns]
if missing:
    st.error(f"Missing expected columns: {missing}")
    st.stop()

df = df_raw.copy()
df["ETD_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["Off-Block (Est)"])
df["ETA_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["On-Block (Est)"])

# Filter out fake/non-tail rows
df["is_real_leg"] = df["Aircraft"].apply(is_real_tail)
df = df[df["is_real_leg"]].copy()

# Classify & format
df["Type"] = df["Account"].apply(classify_account)
df["TypeBadge"] = df["Type"].apply(type_badge)
df["From"] = df["From (ICAO)"].fillna("—").replace({"nan": "—"})
df["To"] = df["To (ICAO)"].fillna("—").replace({"nan": "—"})
df["Route"] = df["From"] + " → " + df["To"]
# df["Sched FT"] = df["Flight time (Est)"].apply(flight_time_hhmm_from_decimal)

# Relative time columns
now_utc = datetime.now(timezone.utc)
df["Departs In"] = (df["ETD_UTC"] - now_utc).apply(fmt_td)
df["Arrives In"] = (df["ETA_UTC"] - now_utc).apply(fmt_td)

# ============================
# Build event map (persisted + in-session) and compute status
# ============================
events_map = load_status_map()
# Overlay in-session updates (from IMAP polling this run) into events_map
if st.session_state.get("status_updates"):
    for b, upd in st.session_state["status_updates"].items():
        et = upd.get("type") or "Unknown"
        events_map.setdefault(b, {})[et] = upd

def compute_status_row(booking, dep_utc, eta_utc) -> str:
    rec = events_map.get(booking, {})
    now = datetime.now(timezone.utc)
    thr = timedelta(minutes=int(delay_threshold_min))

    # Hard-priority events from emails
    if "Diversion" in rec:
        return rec["Diversion"]["status"]
    if "Arrival" in rec:
        return rec["Arrival"]["status"]
    if "ArrivalForecast" in rec:
        return "🟦 ARRIVING SOON"

    # We have a DEP email but not ARR yet
    if "Departure" in rec:
        if pd.notna(eta_utc) and now >= eta_utc + thr:
            return "🟠 LATE ARRIVAL"  # departed, but overdue to arrive
        return "🟢 DEPARTED"         # departed and not yet overdue

    # No emails at all → schedule-based, but DON'T assume movement without email
    if pd.notna(dep_utc):
        if now < dep_utc:
            return "🟡 SCHEDULED"
        # Past ETD with no dep email ⇒ treat as delay (after threshold)
        return "🔴 DELAY" if (now - dep_utc) >= thr else "🟡 SCHEDULED"

    return "🟡 SCHEDULED"

# Compute status per row using the email-driven logic above
df["Status"] = [compute_status_row(b, dep, eta) for b, dep, eta in zip(df["Booking"], df["ETD_UTC"], df["ETA_UTC"])]


# ============================
# Quick Filters
# ============================
st.markdown("### Quick Filters")
tails_opts = sorted(df["Aircraft"].dropna().unique().tolist())
airports_opts = sorted(pd.unique(pd.concat([df["From"].fillna("—"), df["To"].fillna("—")], ignore_index=True)).tolist())
workflows_opts = sorted(df["Workflow"].fillna("").unique().tolist())

f1, f2, f3 = st.columns([1, 1, 1])
with f1:
    tails_sel = st.multiselect("Tail(s)", tails_opts, default=[])
with f2:
    airports_sel = st.multiselect("Airport(s) (matches From OR To)", airports_opts, default=[])
with f3:
    workflows_sel = st.multiselect("Workflow(s)", workflows_opts, default=[])

if tails_sel:
    df = df[df["Aircraft"].isin(tails_sel)]
if airports_sel:
    df = df[df["From"].isin(airports_sel) | df["To"].isin(airports_sel)]
if workflows_sel:
    df = df[df["Workflow"].isin(workflows_sel)]

# Time-window filters
now_utc = datetime.now(timezone.utc)
if limit_next_hours:
    window_end = now_utc + pd.Timedelta(hours=int(next_hours))
    df = df[(df["ETD_UTC"] >= now_utc - pd.Timedelta(minutes=5)) & (df["ETD_UTC"] <= window_end)]
elif show_only_upcoming:
    df = df[df["ETD_UTC"] >= now_utc - pd.Timedelta(minutes=5)]

# Sort & display
df = df.sort_values(by=["ETD_UTC", "ETA_UTC"], ascending=[True, True]).copy()
display_cols = [
    "TypeBadge", "Booking", "Aircraft", "Aircraft Type", "Route",
    "Off-Block (Est)", "On-Block (Est)", "Departs In", "Arrives In",
    "PIC", "SIC", "Workflow", "Status"
]
st.subheader(f"Schedule  ·  {len(df)} flight(s) shown")
st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")
st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

# ============================
# Mailbox Polling (IMAP)
# ============================
st.markdown("### Mailbox Polling")
IMAP_HOST = st.secrets.get("IMAP_HOST")
IMAP_USER = st.secrets.get("IMAP_USER")
IMAP_PASS = st.secrets.get("IMAP_PASS")
IMAP_FOLDER = st.secrets.get("IMAP_FOLDER", "INBOX")
IMAP_SENDER = st.secrets.get("IMAP_SENDER")  # e.g., "noreply@flightbridge.com"

enable_poll = st.checkbox("Enable IMAP polling", value=False,
                          help="Poll the mailbox for FlightBridge alerts and auto-apply updates.")

def imap_poll_once(max_to_process: int = 25) -> int:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        return 0
    M = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        M.login(IMAP_USER, IMAP_PASS)
    except imaplib.IMAP4.error as e:
        st.error(f"IMAP login failed: {e}")
        return 0

    typ, _ = M.select(IMAP_FOLDER)
    if typ != "OK":
        st.error(f"Could not open folder {IMAP_FOLDER}")
        try: M.logout()
        except: pass
        return 0

    # Search for only newer UIDs (server-side) for speed
    last_uid = get_last_uid(IMAP_USER + ":" + IMAP_FOLDER)
    if IMAP_SENDER:
        typ, data = M.uid('search', None, 'FROM', f'"{IMAP_SENDER}"', f'UID {last_uid+1}:*')
    else:
        typ, data = M.uid('search', None, f'UID {last_uid+1}:*')
    if typ != "OK":
        st.error("IMAP search failed")
        M.logout()
        return 0

    uids = [int(x) for x in (data[0].split() if data and data[0] else [])]
    if not uids:
        M.logout()
        return 0

    applied = 0
    for uid in sorted(uids)[:max_to_process]:
        typ, msg_data = M.uid('fetch', str(uid), '(RFC822)')
        if typ != "OK" or not msg_data or not msg_data[0]:
            set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
            continue
        try:
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            subject = msg.get('Subject', '')
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                        break
                if not body:
                    for part in msg.walk():
                        if part.get_content_type().startswith("text/"):
                            body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                            break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode(msg.get_content_charset() or 'utf-8', errors='ignore')

            text = (subject or "") + "\n" + (body or "")
            now_utc = datetime.now(timezone.utc)

            # Subject-first
            subj_info = parse_subject_line(subject or "", now_utc)
            event = subj_info.get("event_type")
            actual_dt_utc = subj_info.get("actual_time_utc")

            # Fallbacks
            if not event:
                event = extract_event(text)
            if actual_dt_utc is None:
                actual_dt_utc = parse_any_datetime_to_utc(text) or now_utc

            # Booking
            bookings = extract_candidates(text)[0]
            booking = bookings[0] if bookings else None

            tails = []
            if subj_info.get("tail"): tails.append(subj_info["tail"])
            tails += re.findall(r"\bC-[A-Z0-9]{4}\b", text)
            tails = sorted(set(tails))

            if not booking and tails and actual_dt_utc:
                rows = df_clean[df_clean["Aircraft"].isin(tails)].copy()
                if not rows.empty:
                    if event in ("Arrival", "ArrivalForecast"):
                        rows["Δ"] = (rows["ETA_UTC"] - actual_dt_utc).abs()
                    else:
                        rows["Δ"] = (rows["ETD_UTC"] - actual_dt_utc).abs()
                    rows = rows.sort_values("Δ")
                    if not rows.empty and rows.iloc[0]["Δ"] <= pd.Timedelta(hours=12):
                        booking = rows.iloc[0]["Booking"]

            if not (booking and event and actual_dt_utc):
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                continue

            row = df_clean[df_clean["Booking"] == booking]
            planned = None
            if not row.empty:
                planned = row["ETA_UTC"].iloc[0] if event in ("Arrival", "ArrivalForecast") else row["ETD_UTC"].iloc[0]
            delta_min = None
            if planned is not None and pd.notna(planned):
                delta_min = int(round((actual_dt_utc - planned).total_seconds() / 60.0))

            if event == "Diversion":
                status = f"🔷 DIVERTED to {subj_info.get('to_airport','—')}"
            elif event == "ArrivalForecast":
                status = "🟦 ARRIVING SOON"
            elif event == "Arrival":
                status = "🟣 ARRIVED" if (delta_min is None or abs(delta_min) < int(delay_threshold_min)) else "🟠 LATE ARRIVAL"
            else:
                status = "🟢 DEPARTED" if (delta_min is None or abs(delta_min) < int(delay_threshold_min)) else "🔴 DELAY"

            st.session_state.setdefault("status_updates", {})
            st.session_state["status_updates"][booking] = {
                "type": event,
                "actual_time_utc": actual_dt_utc.isoformat(),
                "delta_min": delta_min,
                "status": status,
            }
            upsert_status(booking, event, status, actual_dt_utc.isoformat(), delta_min)
            applied += 1
        finally:
            set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)

    try: M.logout()
    except: pass
    return applied

if enable_poll:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        st.warning("Set IMAP_HOST / IMAP_USER / IMAP_PASS (and optionally IMAP_SENDER/IMAP_FOLDER) in Streamlit secrets.")
    else:
        c_poll1, c_poll2 = st.columns([1,1])
        with c_poll1:
            if st.button("Poll now"):
                applied = imap_poll_once()
                st.success(f"Applied {applied} update(s) from mailbox.")
        with c_poll2:
            poll_on_refresh = st.checkbox("Poll automatically on refresh", value=True)
        if poll_on_refresh:
            try:
                applied = imap_poll_once()
                if applied:
                    st.info(f"Auto-poll applied {applied} update(s).")
            except Exception as e:
                st.error(f"Auto-poll error: {e}")
