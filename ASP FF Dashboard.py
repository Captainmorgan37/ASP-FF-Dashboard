# daily_ops_dashboard.py
import re
import sqlite3
import imaplib, email
from email.utils import parsedate_to_datetime
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
            status TEXT NOT NULL,      -- canonical label for that event
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

# Uniform display for UTC datetimes (match CSV look)
def fmt_dt_utc(dt: datetime | None) -> str:
    if not dt or (isinstance(dt, float) and pd.isna(dt)):
        return "—"
    return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M")

def parse_iso_to_utc(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        dt = dateparse.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def default_status(dep_utc: pd.Timestamp, eta_utc: pd.Timestamp) -> str:
    # Kept for reference; not used to auto-flip
    now = datetime.now(timezone.utc)
    if pd.notna(dep_utc) and now < dep_utc:
        return "🟡 SCHEDULED"
    if pd.notna(dep_utc) and now >= dep_utc and (pd.isna(eta_utc) or now < eta_utc):
        return "🟢 DEPARTED"
    if pd.notna(eta_utc) and now >= eta_utc:
        return "🟣 ARRIVED"
    return "🟡 SCHEDULED"

# ---------- Subject-aware parsing ----------
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

# Do not set actual_time_utc from subject for Arrival/Departure/Diversion
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
        return result

    m = SUBJ_PATTERNS["Diversion"].search(subject)
    if m:
        result["event_type"] = "Diversion"
        result["to_airport"] = m.group("to")
        return result

    return result

# ---- Parse explicit datetime anywhere in text ----
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

# Email Date header → UTC
def get_email_date_utc(msg) -> datetime | None:
    try:
        d = msg.get('Date')
        if not d:
            return None
        dt = parsedate_to_datetime(d)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
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

# ---- BODY parsers for first line info ----
BODY_DEPARTURE_RE = re.compile(
    r"departed\s+.*?\((?P<from>[A-Z]{3,4})\)\s+at\s+(?P<dep_time>\d{1,2}:\d{2}\s*[AP]M\s*[A-Z]{2,4})"
    r".*?enroute\s+to\s+.*?\((?P<to>[A-Z]{3,4})\).*?"
    r"(?:estimated\s+arrival\s+(?:at|time\s+of)\s+(?P<eta_time>\d{1,2}:\d{2}\s*[AP]M\s*[A-Z]{2,4}))?",
    re.I
)

BODY_ARRIVAL_RE = re.compile(
    r"arrived\s+at\s+.*?\((?P<at>[A-Z]{3,4})\)\s+at\s+(?P<arr_time>\d{1,2}:\d{2}\s*[AP]M\s*[A-Z]{2,4})"
    r".*?from\s+.*?\((?P<from>[A-Z]{3,4})\)",
    re.I
)

def _parse_time_token_to_utc(time_token: str, base_date_utc: datetime) -> datetime | None:
    if not time_token:
        return None
    base_day = (base_date_utc or datetime.now(timezone.utc)).date()
    s = f"{base_day.isoformat()} {time_token}"
    try:
        dt = dateparse.parse(s, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def parse_body_firstline(event: str, body: str, email_date_utc: datetime) -> dict:
    info = {}
    if not body:
        return info
    m = BODY_DEPARTURE_RE.search(body)
    if event == "Departure" and m:
        info["from"] = m.group("from")
        info["to"] = m.group("to")
        info["dep_time_utc"] = _parse_time_token_to_utc(m.group("dep_time"), email_date_utc)
        if m.group("eta_time"):
            info["eta_time_utc"] = _parse_time_token_to_utc(m.group("eta_time"), email_date_utc)
        return info
    m = BODY_ARRIVAL_RE.search(body)
    if event == "Arrival" and m:
        info["at"] = m.group("at")
        info["from"] = m.group("from")
        info["arr_time_utc"] = _parse_time_token_to_utc(m.group("arr_time"), email_date_utc)
        return info
    return info

# ---- Airport-aware, tight-window chooser for booking when not in email ----
def choose_booking_for_event(subj_info: dict, tails: list[str], event: str, event_dt_utc: datetime) -> str | None:
    if event_dt_utc is None or event not in ("Arrival", "ArrivalForecast", "Departure", "Diversion"):
        return None
    cand = df_clean.copy()

    # Narrow by tail if present
    if tails:
        cand = cand[cand["Aircraft"].isin(tails)]
        if cand.empty:
            return None

    # Narrow by airports
    at_ap   = subj_info.get("at_airport")
    from_ap = subj_info.get("from_airport")
    to_ap   = subj_info.get("to_airport")

    if event in ("Arrival", "ArrivalForecast"):
        if at_ap:   cand = cand[cand["To"] == at_ap]
        if from_ap: cand = cand[cand["From"] == from_ap]
        sched_col = "ETA_UTC"
    elif event == "Departure":
        if from_ap: cand = cand[cand["From"] == from_ap]
        if to_ap:   cand = cand[cand["To"] == to_ap]
        sched_col = "ETD_UTC"
    else:  # Diversion → match pre-diversion leg; use dep side if given
        if from_ap: cand = cand[cand["From"] == from_ap]
        sched_col = "ETD_UTC"

    if cand.empty:
        return None

    cand = cand.copy()
    cand["Δ"] = (cand[sched_col] - event_dt_utc).abs()
    cand = cand.sort_values("Δ")

    MAX_WINDOW = pd.Timedelta(hours=3)
    best = cand.iloc[0]
    return best["Booking"] if best["Δ"] <= MAX_WINDOW else None

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
            f"(uploaded {st.session_state['uploaded_at']})"
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

# Relative time columns (initial; we'll blank after email checks)
now_utc = datetime.now(timezone.utc)
df["Departs In"] = (df["ETD_UTC"] - now_utc).apply(fmt_td)
df["Arrives In"] = (df["ETA_UTC"] - now_utc).apply(fmt_td)

# Keep pre-filter copy for matching
df_clean = df.copy()

# ============================
# Email-driven status + enrich FA times
# ============================
events_map = load_status_map()
# Overlay in-session updates (from this run's IMAP polling)
if st.session_state.get("status_updates"):
    for b, upd in st.session_state["status_updates"].items():
        et = upd.get("type") or "Unknown"
        events_map.setdefault(b, {})[et] = upd

def compute_status_row(booking, dep_utc, eta_utc) -> str:
    rec = events_map.get(booking, {})
    now = datetime.now(timezone.utc)
    thr = timedelta(minutes=int(delay_threshold_min))

    has_dep  = "Departure" in rec
    has_arr  = "Arrival" in rec
    has_div  = "Diversion" in rec

    # DIVERTED only if a Departure email exists and a Diversion email arrives after
    if has_dep and has_div:
        return rec["Diversion"].get("status", "🔷 DIVERTED")

    # ARRIVED if Arrival email exists
    if has_arr:
        return "🟣 ARRIVED"

    # DEPARTED / LATE ARRIVAL if Departure email exists but no Arrival yet
    if has_dep and not has_arr:
        # Use forecast ETA if available, else scheduled ETA
        fore_eta = None
        if "ArrivalForecast" in rec:
            fore_eta = parse_iso_to_utc(rec["ArrivalForecast"].get("actual_time_utc"))
        eta_for_status = fore_eta or eta_utc
        if pd.notna(eta_for_status) and now > eta_for_status + thr:
            return "🟠 LATE ARRIVAL"
        return "🟢 DEPARTED"

    # No Departure email yet: SCHEDULED (grace until ETD+thr) else DELAY
    if pd.notna(dep_utc):
        return "🟡 SCHEDULED" if now <= dep_utc + thr else "🔴 DELAY"

    return "🟡 SCHEDULED"

# Compute Status
df["Status"] = [compute_status_row(b, dep, eta) for b, dep, eta in zip(df["Booking"], df["ETD_UTC"], df["ETA_UTC"])]

# Build FA time columns from events_map
dep_actual_list = []
eta_fore_list   = []
arr_actual_list = []

for b in df["Booking"]:
    rec = events_map.get(b, {})
    dep_actual_list.append(parse_iso_to_utc(rec.get("Departure", {}).get("actual_time_utc")))
    eta_fore_list.append(parse_iso_to_utc(rec.get("ArrivalForecast", {}).get("actual_time_utc")))
    arr_actual_list.append(parse_iso_to_utc(rec.get("Arrival", {}).get("actual_time_utc")))

df["Off-Block (Actual)"] = [fmt_dt_utc(x) for x in dep_actual_list]
df["ETA (FA)"]           = [fmt_dt_utc(x) for x in eta_fore_list]
df["On-Block (Actual)"]  = [fmt_dt_utc(x) for x in arr_actual_list]

# Blank countdowns when appropriate
has_dep_series = df["Booking"].map(lambda b: "Departure" in events_map.get(b, {}))
has_arr_series = df["Booking"].map(lambda b: "Arrival" in events_map.get(b, {}))
df.loc[has_dep_series, "Departs In"] = "—"
df.loc[has_arr_series, "Arrives In"] = "—"

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
    "Off-Block (Est)", "Off-Block (Actual)", "ETA (FA)",
    "On-Block (Est)", "On-Block (Actual)",
    "Departs In", "Arrives In",
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

    # Search only newer UIDs (server-side)
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

            # Event time precedence: body OUT/IN (if found) > explicit timestamp in text > subject-derived (forecast only) > email Date header > now
            hdr_dt = get_email_date_utc(msg)
            explicit_dt = parse_any_datetime_to_utc(text)
            body_info = parse_body_firstline(event, body, hdr_dt or now_utc)

            if event == "Departure":
                actual_dt_utc = body_info.get("dep_time_utc") or explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc
            elif event == "Arrival":
                actual_dt_utc = body_info.get("arr_time_utc") or explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc
            else:
                actual_dt_utc = explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc

            # Enrich airports from body if present
            if body_info.get("from") and not subj_info.get("from_airport"):
                subj_info["from_airport"] = body_info["from"]
            if body_info.get("to") and not subj_info.get("to_airport"):
                subj_info["to_airport"] = body_info["to"]
            if body_info.get("at") and not subj_info.get("at_airport"):
                subj_info["at_airport"] = body_info["at"]

            # Booking directly from text; else choose by tail+airports+tight window
            bookings = extract_candidates(text)[0]
            booking = bookings[0] if bookings else None

            tails = []
            if subj_info.get("tail"): tails.append(subj_info["tail"])
            tails += re.findall(r"\bC-[A-Z0-9]{4}\b", text)
            tails = sorted(set(tails))

            if not booking:
                booking = choose_booking_for_event(subj_info, tails, event, actual_dt_utc)

            # If still no confident match, skip to avoid mis-assigning
            if not (booking and event and actual_dt_utc):
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                continue

            # Compute delta_min for reference (not used to decide canonical status)
            row = df_clean[df_clean["Booking"] == booking]
            planned = None
            if not row.empty:
                planned = row["ETA_UTC"].iloc[0] if event in ("Arrival", "ArrivalForecast") else row["ETD_UTC"].iloc[0]
            delta_min = None
            if planned is not None and pd.notna(planned):
                delta_min = int(round((actual_dt_utc - planned).total_seconds() / 60.0))

            # Canonical event -> persisted status (facts only)
            if event == "Diversion":
                status = f"🔷 DIVERTED to {subj_info.get('to_airport','—')}"
            elif event == "ArrivalForecast":
                status = "🟦 ARRIVING SOON"
            elif event == "Arrival":
                status = "🟣 ARRIVED"
            elif event == "Departure":
                status = "🟢 DEPARTED"
            else:
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                continue

            # Persist/update the primary event
            st.session_state.setdefault("status_updates", {})
            st.session_state["status_updates"][booking] = {
                "type": event,
                "actual_time_utc": actual_dt_utc.isoformat(),
                "delta_min": delta_min,
                "status": status,
            }
            upsert_status(booking, event, status, actual_dt_utc.isoformat(), delta_min)

            # If a Departure email contains an updated ETA, persist it as an ArrivalForecast event
            if event == "Departure" and body_info.get("eta_time_utc"):
                eta_iso = body_info["eta_time_utc"].isoformat()
                st.session_state["status_updates"][booking] = {
                    **st.session_state["status_updates"].get(booking, {}),
                    "type": "ArrivalForecast",
                    "actual_time_utc": eta_iso,
                    "delta_min": None,
                    "status": "🟦 ARRIVING SOON",
                }
                upsert_status(booking, "ArrivalForecast", "🟦 ARRIVING SOON", eta_iso, None)

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
