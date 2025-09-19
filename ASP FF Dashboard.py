# ASP FF Dashboard.py
# Streamlit dashboard for daily ops (Schedule + Status) with IMAP ingestion
# and per-row TELUS Business Connect (RingCentral) Team Notification buttons.

import re
import sqlite3
import imaplib, email
from io import BytesIO
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from dateutil import parser as dateparse
from email import policy
from email.parser import BytesParser

# Extra libs for notifications and local time formatting
import requests
import tzlocal


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
# SQLite persistence (statuses + CSV + email cursor)
# ============================
DB_PATH = "status_store.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS status_events (
            booking TEXT NOT NULL,
            event_type TEXT NOT NULL,  -- 'Departure' | 'Arrival' | 'ArrivalForecast' | 'Diversion' | 'EDCT'
            status TEXT NOT NULL,      -- human status string
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

def delete_status(booking: str, event_type: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM status_events WHERE booking=? AND event_type=?", (booking, event_type))

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

def fmt_dt_utc(ts) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    return pd.to_datetime(ts).tz_convert(timezone.utc).strftime("%Y-%m-%d %H:%MZ")

# ----- Tail normalization (accept CFSRY and C-FSRY) -----
SUBJ_TAIL_RE = re.compile(r"\bC-?[A-Z0-9]{4}\b")
def normalize_tail_token(t: str) -> str:
    t = (t or "").strip().upper()
    if re.fullmatch(r"C-?[A-Z0-9]{4}", t):
        return f"C-{t[-4:]}"
    return t

# ----- IATA/ICAO small helpers -----
def normalize_iata(tok: str | None) -> str | None:
    if not tok:
        return None
    s = tok.strip().upper()
    if len(s) == 3 and s.isalnum():
        return s
    return None

def normalize_icao(tok: str | None) -> str | None:
    if not tok:
        return None
    s = tok.strip().upper()
    if len(s) == 4 and s.isalnum():
        return s
    return None

# ----- TZ mapping for email bodies (MDT/EDT/etc.) -----
TZ_ABBR_OFFSETS = {
    "PST": -8, "PDT": -7,
    "MST": -7, "MDT": -6,
    "CST": -6, "CDT": -5,
    "EST": -5, "EDT": -4,
    "AKDT": -8, "AKST": -9,
    "HST": -10,
}
def parse_body_time_to_utc(date_text: str, time_text: str, tz_text: str, sent_dt_utc: datetime) -> datetime | None:
    """Try to build a UTC datetime from pieces found in the body (with tz abbr)."""
    try:
        # Prefer explicit date+time; fall back to same date as email 'Sent'
        if date_text:
            base = dateparse.parse(date_text, fuzzy=True)
        else:
            base = sent_dt_utc.astimezone(timezone.utc)
        # Parse time
        tm = dateparse.parse(time_text, fuzzy=True).time()
        dt_local = datetime(base.year, base.month, base.day, tm.hour, tm.minute)
        tz_abbr = (tz_text or "").strip().upper()
        if tz_abbr in TZ_ABBR_OFFSETS:
            offset = TZ_ABBR_OFFSETS[tz_abbr]
            return (dt_local - timedelta(hours=offset)).replace(tzinfo=timezone.utc)
        # Try letting dateutil do it if includes tz
        dt_try = dateparse.parse(f"{date_text} {time_text} {tz_text}".strip(), fuzzy=True)
        if dt_try.tzinfo is None:
            dt_try = dt_try.replace(tzinfo=timezone.utc)
        return dt_try.astimezone(timezone.utc)
    except Exception:
        return None

# ----- Subject/body parsing -----
SUBJ_CALLSIGN_RE = re.compile(r"\bASP\d{3,4}\b", re.I)

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
    tail = normalize_tail_token(tail_m.group(0)) if tail_m else None
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
    # dd.mm.yyyy HH:MM + tz abbr in body
    m_date = re.search(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b", text)
    m_time = re.search(r"\b(\d{1,2}:\d{2}(?:\s*[APap][Mm])?)\b", text)
    m_tz   = re.search(r"\b([A-Z]{2,4})\b", text)
    if m_time:
        base = datetime.now(timezone.utc)
        return parse_body_time_to_utc(m_date.group(0) if m_date else base.strftime("%Y-%m-%d"),
                                      m_time.group(0), m_tz.group(0) if m_tz else "UTC", base)
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
    tails = [normalize_tail_token(x) for x in re.findall(r"\bC-?[A-Z0-9]{4}\b", text)]
    tails = sorted(set(tails))
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

# Priority 3: DB fallback
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

# Optional IATA columns (helpful for emails that only include IATA)
has_iata = ("From (IATA)" in df_raw.columns) and ("To (IATA)" in df_raw.columns)

df = df_raw.copy()
df["ETD_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["Off-Block (Est)"])
df["ETA_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["On-Block (Est)"])

# Filter out fake/non-tail rows
df["is_real_leg"] = df["Aircraft"].apply(is_real_tail)
df = df[df["is_real_leg"]].copy()

# Classify & format
df["Type"] = df["Account"].apply(classify_account)
df["TypeBadge"] = df["Type"].apply(type_badge)

# Display From/To with IATA fallback if ICAO missing
df["From"] = df["From (ICAO)"].fillna("")
df["To"] = df["To (ICAO)"].fillna("")
if has_iata:
    df.loc[df["From"].eq("") | df["From"].eq("nan"), "From"] = df["From (IATA)"].fillna("—")
    df.loc[df["To"].eq("") | df["To"].eq("nan"), "To"] = df["To (IATA)"].fillna("—")
df["From"] = df["From"].replace({"": "—", "nan": "—"})
df["To"] = df["To"].replace({"": "—", "nan": "—"})
df["Route"] = df["From"] + " → " + df["To"]

# Relative time columns
now_utc = datetime.now(timezone.utc)
df["Departs In"] = (df["ETD_UTC"] - now_utc).apply(fmt_td)
df["Arrives In"] = (df["ETA_UTC"] - now_utc).apply(fmt_td)

# Default status (purely scheduled)
def default_status(dep_utc: pd.Timestamp, eta_utc: pd.Timestamp) -> str:
    now = datetime.now(timezone.utc)
    if pd.notna(dep_utc) and now < dep_utc:
        return "🟡 SCHEDULED"
    if pd.notna(dep_utc) and now >= dep_utc and (pd.isna(eta_utc) or now < eta_utc):
        return "🟢 DEPARTED"
    if pd.notna(eta_utc) and now >= eta_utc:
        return "🟣 ARRIVED"
    return "🟡 SCHEDULED"

df["Status"] = [default_status(dep, eta) for dep, eta in zip(df["ETD_UTC"], df["ETA_UTC"])]

# Keep pre-filter copy for lookups
df_clean = df.copy()


# ============================
# Merge persisted statuses into working df
# Precedence: Diversion > Arrival > ArrivalForecast > Departure > default
# ============================
persisted = load_status_map()

def merged_status(booking, default_val):
    rec = persisted.get(booking, {})
    if "Diversion" in rec: return rec["Diversion"]["status"]
    if "Arrival" in rec: return rec["Arrival"]["status"]
    if "ArrivalForecast" in rec: return rec["ArrivalForecast"]["status"]
    if "Departure" in rec: return rec["Departure"]["status"]
    if "EDCT" in rec: return rec["EDCT"]["status"]
    return default_val

df["Status"] = [merged_status(b, s) for b, s in zip(df["Booking"], df["Status"])]

# session overlay
if "status_updates" in st.session_state and st.session_state["status_updates"]:
    su = st.session_state["status_updates"]
    df["Status"] = [su.get(b, {}).get("status", s) for b, s in zip(df["Booking"], df["Status"])]

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

# ============================
# Derive FA actuals / forecasts for styling/columns
# ============================
events_map = load_status_map()  # latest persisted
def parse_iso_to_utc(s: str | None) -> pd.Timestamp | None:
    if not s: return None
    try:
        dt = dateparse.parse(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return pd.Timestamp(dt.astimezone(timezone.utc))
    except Exception:
        return None

# pull into working df
events_map = load_status_map()

def _to_utc_ts(s: str | None) -> pd.Timestamp | None:
    if not s:
        return pd.NaT
    try:
        dt = dateparse.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return pd.Timestamp(dt.astimezone(timezone.utc))
    except Exception:
        return pd.NaT

df["_DepActual_ts"] = df["Booking"].map(lambda b: _to_utc_ts(events_map.get(b, {}).get("Departure", {}).get("actual_time_utc")))
df["_ArrActual_ts"] = df["Booking"].map(lambda b: _to_utc_ts(events_map.get(b, {}).get("Arrival", {}).get("actual_time_utc")))
df["_ETA_FA_ts"]   = df["Booking"].map(lambda b: _to_utc_ts(events_map.get(b, {}).get("ArrivalForecast", {}).get("actual_time_utc")))
df["_EDCT_ts"]     = df["Booking"].map(lambda b: _to_utc_ts(events_map.get(b, {}).get("EDCT", {}).get("actual_time_utc")))

# Ensure dtype is datetime64[ns, UTC] (not object)
for c in ["_DepActual_ts", "_ArrActual_ts", "_ETA_FA_ts", "_EDCT_ts"]:
    df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")


# blank countdowns when actuals exist
df["Departs In"] = df.apply(lambda r: "—" if pd.notna(r["_DepActual_ts"]) else fmt_td(r["ETD_UTC"] - now_utc), axis=1)
df["Arrives In"] = df.apply(lambda r: "—" if pd.notna(r["_ArrActual_ts"]) else fmt_td(r["ETA_UTC"] - now_utc), axis=1)

# ============================
# Post-arrival visibility controls
# ============================
v1, v2, v3 = st.columns([1, 1, 1])
with v1:
    highlight_recent_arrivals = st.checkbox("Highlight recently landed", value=True)
with v2:
    highlight_minutes = st.number_input("Highlight window (min)", min_value=10, max_value=240, value=60, step=5)
with v3:
    auto_hide_landed = st.checkbox("Auto-hide landed", value=True)
hide_hours = st.number_input("Hide landed after (hours)", min_value=1, max_value=24, value=2, step=1)

# Hide older landed
if auto_hide_landed:
    cutoff_hide = now_utc - pd.Timedelta(hours=int(hide_hours))
    df = df[~(df["_ArrActual_ts"].notna() & (df["_ArrActual_ts"] < cutoff_hide))].copy()

# recompute presence masks on the filtered df
has_dep_series = df["Booking"].map(lambda b: "Departure" in events_map.get(b, {}))
has_arr_series = df["Booking"].map(lambda b: "Arrival" in events_map.get(b, {}))

# ============================
# Sort, compute row/cell highlights, display
# ============================
df = df.sort_values(by=["ETD_UTC", "ETA_UTC"], ascending=[True, True]).copy()
df["_orig_order"] = range(len(df))

delay_thr_td    = pd.Timedelta(minutes=int(delay_threshold_min))   # 15m default
row_red_thr_td  = pd.Timedelta(minutes=max(30, int(delay_threshold_min)))  # 30m+

now_utc = datetime.now(timezone.utc)

# Row-level operational "no-email" delays
no_dep = ~has_dep_series
dep_lateness = now_utc - df["ETD_UTC"]
row_dep_yellow = df["ETD_UTC"].notna() & no_dep & (dep_lateness > delay_thr_td) & (dep_lateness < row_red_thr_td)
row_dep_red    = df["ETD_UTC"].notna() & no_dep & (dep_lateness >= row_red_thr_td)

has_dep = has_dep_series
no_arr = ~has_arr_series
eta_baseline = df["_ETA_FA_ts"].where(df["_ETA_FA_ts"].notna(), df["ETA_UTC"])
eta_lateness = now_utc - eta_baseline
row_arr_yellow = eta_baseline.notna() & has_dep & no_arr & (eta_lateness > delay_thr_td) & (eta_lateness < row_red_thr_td)
row_arr_red    = eta_baseline.notna() & has_dep & no_arr & (eta_lateness >= row_red_thr_td)

row_yellow = row_dep_yellow | row_arr_yellow
row_red    = row_dep_red    | row_arr_red

# Cell-level red accents (variance rules)
dep_delay        = df["_DepActual_ts"] - df["ETD_UTC"]   # Off-Block (Actual) - Off-Block (Est)
eta_fa_vs_sched  = df["_ETA_FA_ts"]    - df["ETA_UTC"]   # ETA (FA) - On-Block (Est)
arr_vs_sched     = df["_ArrActual_ts"] - df["ETA_UTC"]   # On-Block (Actual) - On-Block (Est)

cell_dep = dep_delay.notna()       & (dep_delay       > delay_thr_td)
cell_eta = eta_fa_vs_sched.notna() & (eta_fa_vs_sched > delay_thr_td)
cell_arr = arr_vs_sched.notna()    & (arr_vs_sched    > delay_thr_td)

# ---- New: Delayed View controls next to the title ----
head_toggle_col, head_title_col = st.columns([1.6, 8.4])
with head_toggle_col:
    delayed_view = st.checkbox("Delayed View", value=False, help="Show RED (≥30m) first, then YELLOW (15–29m).")
    hide_non_delayed = st.checkbox("Hide non-delayed", value=True, help="Only show red/yellow rows.") if delayed_view else False
with head_title_col:
    st.subheader("Schedule")

# Compute a delay priority (2 = red, 1 = yellow, 0 = normal)
delay_priority = (row_red.astype(int) * 2 + row_yellow.astype(int))
df["_DelayPriority"] = delay_priority

# If Delayed View is on: optionally filter, then sort by priority
df_view = df.copy()
if delayed_view:
    if hide_non_delayed:
        df_view = df_view[df_view["_DelayPriority"] > 0].copy()
    df_view = df_view.sort_values(by=["_DelayPriority", "_orig_order"], ascending=[False, True])

display_cols = [
    "TypeBadge", "Booking", "Aircraft", "Aircraft Type", "Route",
    "Off-Block (Est)", "Off-Block (Actual)", "ETA (FA)",
    "On-Block (Est)", "On-Block (Actual)",
    "Departs In", "Arrives In",
    "PIC", "SIC", "Workflow", "Status"
]

# Prepare display frame (with FA/scheduled columns materialized)
df_display_base = df_view if delayed_view else df
df_display = df_display_base.copy()

def _fmt_dep_actual(row):
    if pd.notna(row["_DepActual_ts"]):
        return row["_DepActual_ts"].strftime("%Y-%m-%d %H:%MZ")
    if pd.notna(row["_EDCT_ts"]):
        return "EDCT " + row["_EDCT_ts"].strftime("%Y-%m-%d %H:%MZ")
    return "—"
df_display["Off-Block (Actual)"] = df_display.apply(_fmt_dep_actual, axis=1)

df_display["ETA (FA)"] = df_display["_ETA_FA_ts"].apply(lambda x: x.strftime("%Y-%m-%d %H:%MZ") if pd.notna(x) else "—")
df_display["On-Block (Actual)"] = df_display["_ArrActual_ts"].apply(lambda x: x.strftime("%Y-%m-%d %H:%MZ") if pd.notna(x) else "—")

df_display = df_display[display_cols]

# Masks for green overlay computed outside styler
recent_cut = datetime.now(timezone.utc) - pd.Timedelta(minutes=int(highlight_minutes))
row_green = df_display_base["_ArrActual_ts"].notna() & (df_display_base["_ArrActual_ts"] >= recent_cut)

def _style_ops(x: pd.DataFrame):
    styles = pd.DataFrame("", index=x.index, columns=x.columns)

    # 1) Row backgrounds: YELLOW then RED
    row_y_css = "background-color: rgba(255, 193, 7, 0.18); border-left: 6px solid #ffc107;"
    row_r_css = "background-color: rgba(255, 82, 82, 0.18); border-left: 6px solid #ff5252;"
    styles.loc[row_yellow.reindex(x.index, fill_value=False), :] = row_y_css
    styles.loc[row_red.reindex(x.index,    fill_value=False), :] = row_r_css

    # 2) GREEN overlay for recent arrivals (after Y/R so it wins at row level)
    if highlight_recent_arrivals:
        row_g_css = "background-color: rgba(76, 175, 80, 0.18); border-left: 6px solid #4caf50;"
        styles.loc[row_green.reindex(x.index, fill_value=False), :] = row_g_css

    # 3) Cell-level red accents (apply after row colors, so cells stay red even on green rows)
    cell_css = "background-color: rgba(255, 82, 82, 0.25);"
    styles.loc[cell_dep.reindex(x.index, fill_value=False), "Off-Block (Actual)"] += cell_css
    styles.loc[cell_eta.reindex(x.index, fill_value=False), "ETA (FA)"] += cell_css
    styles.loc[cell_arr.reindex(x.index, fill_value=False), "On-Block (Actual)"] += cell_css

    # 4) EDCT: purple on Off-Block (Actual) if EDCT exists & no true departure yet
    cell_edct_css = "background-color: rgba(155, 81, 224, 0.28); border-left: 6px solid #9b51e0;"
    idx_edct = (df_display_base["_EDCT_ts"].notna() & df_display_base["_DepActual_ts"].isna()).reindex(x.index, fill_value=False)
    styles.loc[idx_edct, "Off-Block (Actual)"] += cell_edct_css

    return styles

# ============================
# Per-row TELUS BC "Notify" buttons (Incoming Webhook)
# Add your webhooks in .streamlit/secrets.toml:
# [TELUS_WEBHOOKS]
# Ops-Day = "https://hooks.ringcentral.com/webhook/xxxx"
# Ops-Night = "https://hooks.ringcentral.com/webhook/yyyy"
# ============================
def _build_delay_msg(tail: str, booking: str, minutes_delta: int, new_eta_hhmm: str) -> str:
    tail_disp = (tail or "").replace("-", "").upper()
    label = "LATE" if int(minutes_delta) >= 0 else "EARLY"
    mins = abs(int(minutes_delta))
    s = re.sub(r"[^0-9]", "", new_eta_hhmm or "")
    eta_lt = (s if len(s) == 4 else s.zfill(4)) + " LT"
    return (
        f"TAIL#/BOOKING#: {tail_disp}//{booking}\n"
        f"{label}: {mins} minutes\n"
        f"UPDATED ETA: {eta_lt}"
    )

def _post_to_telus_team(team: str, text: str) -> tuple[bool, str]:
    url = st.secrets.get("TELUS_WEBHOOKS", {}).get(team)
    if not url:
        return False, f"No webhook configured for team '{team}'."
    try:
        r = requests.post(url, json={"text": text}, timeout=10)
        ok = 200 <= r.status_code < 300
        return ok, ("" if ok else f"{r.status_code}: {r.text[:200]}")
    except Exception as e:
        return False, str(e)

def notify_delay_chat(team: str, tail: str, booking: str, minutes_delta: int, new_eta_hhmm: str):
    msg = _build_delay_msg(tail, booking, minutes_delta, new_eta_hhmm)
    ok, err = _post_to_telus_team(team, msg)
    if ok:
        st.success(f"Posted: {booking} → {team}")
    else:
        st.error(f"Post failed: {err}")

# Left column: per-row notify popovers
_source_df = df_view if delayed_view else df
_show_df = _source_df.reset_index(drop=True).copy()

local_tz = tzlocal.get_localzone()
def _hhmm_local(ts: pd.Timestamp | None) -> str:
    if ts is None or pd.isna(ts): return ""
    if ts.tzinfo:
        return ts.tz_convert(local_tz).strftime("%H%M")
    return pd.Timestamp(ts, tz="UTC").tz_convert(local_tz).strftime("%H%M")

def _default_minutes_delta(row) -> int:
    # Prefer ETA(FA) vs schedule. Positive => LATE, Negative => EARLY.
    if pd.notna(row.get("_ETA_FA_ts")) and pd.notna(row.get("ETA_UTC")):
        return int(round((row["_ETA_FA_ts"] - row["ETA_UTC"]).total_seconds() / 60.0))
    return 0

def _default_eta_hhmm(row) -> str:
    ts = row.get("_ETA_FA_ts") if pd.notna(row.get("_ETA_FA_ts")) else row.get("ETA_UTC")
    return _hhmm_local(ts)

act_col, table_col = st.columns([1.2, 8.8])
with act_col:
    st.markdown("**Notify**")
    teams = list(st.secrets.get("TELUS_WEBHOOKS", {}).keys())
    if not teams:
        st.warning("Add TELUS_WEBHOOKS to secrets to enable notifications.")
    else:
        for i, row in _show_df.iterrows():
            booking = str(row["Booking"])
            tail = str(row["Aircraft"])
            default_delta = _default_minutes_delta(row)
            default_eta = _default_eta_hhmm(row)
            key_base = f"notify_{booking}_{i}"
            with st.popover("📣", key=f"{key_base}_pop"):
                team = st.selectbox("Team", teams, key=f"{key_base}_team", index=0)
                st.caption(f"Booking **{booking}** · Tail **{tail}**")
                delta = st.number_input("Δ minutes (+late / -early)", value=default_delta, step=1, key=f"{key_base}_delta")
                eta_hhmm = st.text_input("Updated ETA (HHMM / HH:MM)", value=default_eta, key=f"{key_base}_eta")
                if st.button("Send", key=f"{key_base}_send"):
                    notify_delay_chat(team, tail, booking, int(delta), eta_hhmm)
                    st.experimental_rerun()

with table_col:
    st.subheader(f"Schedule  ·  {len(df_display)} flight(s) shown")
    st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")
    st.dataframe(
        df_display.style.hide(axis="index").apply(_style_ops, axis=None),
        use_container_width=True
    )
    if delayed_view and hide_non_delayed:
        st.caption("Delayed View: showing only **RED** (≥30m) and **YELLOW** (15–29m) flights.")
    elif delayed_view:
        st.caption("Delayed View: **RED** (≥30m) first, then **YELLOW** (15–29m); others follow in schedule order.")
    else:
        st.caption(
            "Row colors (operational): **yellow** = 15–29 min late without matching email, **red** = ≥30 min late. "
            "Cell accents: red = variance (Off-Block Actual>Est, ETA(FA)>On-Block Est, On-Block Actual>Est). "
            "EDCT shows in purple in Off-Block (Actual) until a Departure email is received."
        )

# ============================
# Manual Overrides (Departure / Arrival)
# ============================
st.markdown("### Manual Overrides (Departure / Arrival)")
with st.expander("Set or clear an actual OUT / IN time when an email is missing"):
    if df_clean.empty:
        st.info("No flights loaded.")
    else:
        col_ov1, col_ov2 = st.columns([2, 1])
        with col_ov1:
            sel_booking = st.selectbox("Booking", sorted(df_clean["Booking"].astype(str).unique().tolist()))
            row = df_clean[df_clean["Booking"] == sel_booking].iloc[0]
            planned_dep = row["ETD_UTC"]
            planned_arr = row["ETA_UTC"]
            st.caption(
                f"Planned: Off-Block (Est) **{fmt_dt_utc(planned_dep) if pd.notna(planned_dep) else '—'}**, "
                f"On-Block (Est) **{fmt_dt_utc(planned_arr) if pd.notna(planned_arr) else '—'}**"
            )
        with col_ov2:
            set_dep = st.checkbox("Set Actual Departure (OUT)")
            set_arr = st.checkbox("Set Actual Arrival (IN)")

        # Single-box stateful UTC picker (accepts 1005 / 10:05 / 4pm)
        def utc_datetime_picker(label: str, key: str, initial_dt_utc: datetime | None = None) -> datetime:
            if initial_dt_utc is None:
                initial_dt_utc = datetime.now(timezone.utc)
            date_key = f"{key}__date"
            time_txt_key = f"{key}__time_txt"
            time_obj_key = f"{key}__time_obj"
            if date_key not in st.session_state:
                st.session_state[date_key] = initial_dt_utc.date()
            if time_obj_key not in st.session_state:
                st.session_state[time_obj_key] = initial_dt_utc.time().replace(microsecond=0)
            if time_txt_key not in st.session_state:
                st.session_state[time_txt_key] = st.session_state[time_obj_key].strftime("%H:%M")
            d = st.date_input(f"{label} — Date (UTC)", key=date_key)
            txt = st.text_input(f"{label} — Time (UTC)", key=time_txt_key, placeholder="e.g., 1005 or 10:05 or 4pm")
            def _parse_loose_time(s: str):
                s = (s or "").strip().lower().replace(" ", "")
                if not s: return None
                ampm = None
                if s.endswith("am") or s.endswith("pm"):
                    ampm = s[-2:]; s = s[:-2]
                hh = mm = None
                if ":" in s:
                    try:
                        hh_str, mm_str = s.split(":", 1)
                        hh, mm = int(hh_str), int(mm_str)
                    except Exception:
                        return None
                elif s.isdigit():
                    if len(s) in (3, 4):
                        s = s.zfill(4); hh, mm = int(s[:2]), int(s[2:])
                    elif len(s) in (1, 2):
                        hh, mm = int(s), 0
                    else:
                        return None
                else:
                    return None
                if ampm:
                    if hh == 12: hh = 0
                    if ampm == "pm": hh += 12
                if not (0 <= hh <= 23 and 0 <= mm <= 59): return None
                return datetime.strptime(f"{hh:02d}:{mm:02d}", "%H:%M").time()
            parsed = _parse_loose_time(txt)
            if parsed is not None:
                st.session_state[time_obj_key] = parsed
            return datetime.combine(st.session_state[date_key], st.session_state[time_obj_key]).replace(tzinfo=timezone.utc)

        override_dep_dt = None
        override_arr_dt = None
        if set_dep:
            dep_seed = (row["ETD_UTC"].to_pydatetime() if pd.notna(row["ETD_UTC"]) else datetime.now(timezone.utc))
            override_dep_dt = utc_datetime_picker("Actual Departure (UTC)", key="override_dep", initial_dt_utc=dep_seed)
        if set_arr:
            arr_seed = (row["ETA_UTC"].to_pydatetime() if pd.notna(row["ETA_UTC"]) else datetime.now(timezone.utc))
            override_arr_dt = utc_datetime_picker("Actual Arrival (UTC)", key="override_arr", initial_dt_utc=arr_seed)

        cbtn1, cbtn2, _ = st.columns([1,1,2])
        with cbtn1:
            if st.button("Save override(s)"):
                st.session_state.setdefault("status_updates", {})
                if set_dep and override_dep_dt:
                    delta_min = None
                    if pd.notna(planned_dep):
                        delta_min = int(round((override_dep_dt - planned_dep).total_seconds() / 60.0))
                    upsert_status(sel_booking, "Departure", "🟢 DEPARTED", override_dep_dt.isoformat(), delta_min)
                    st.session_state["status_updates"][sel_booking] = {
                        **st.session_state["status_updates"].get(sel_booking, {}),
                        "type": "Departure",
                        "actual_time_utc": override_dep_dt.isoformat(),
                        "delta_min": delta_min,
                        "status": "🟢 DEPARTED",
                    }
                if set_arr and override_arr_dt:
                    delta_min = None
                    if pd.notna(planned_arr):
                        delta_min = int(round((override_arr_dt - planned_arr).total_seconds() / 60.0))
                    upsert_status(sel_booking, "Arrival", "🟣 ARRIVED", override_arr_dt.isoformat(), delta_min)
                    st.session_state["status_updates"][sel_booking] = {
                        **st.session_state["status_updates"].get(sel_booking, {}),
                        "type": "Arrival",
                        "actual_time_utc": override_arr_dt.isoformat(),
                        "delta_min": delta_min,
                        "status": "🟣 ARRIVED",
                    }
                st.success("Override(s) saved. The table will reflect this immediately.")
        with cbtn2:
            if st.button("Clear selected"):
                if set_dep:
                    delete_status(sel_booking, "Departure")
                    if st.session_state.get("status_updates", {}).get(sel_booking, {}).get("type") == "Departure":
                        st.session_state["status_updates"].pop(sel_booking, None)
                if set_arr:
                    delete_status(sel_booking, "Arrival")
                    if st.session_state.get("status_updates", {}).get(sel_booking, {}).get("type") == "Arrival":
                        st.session_state["status_updates"].pop(sel_booking, None)
                st.success("Selected override(s) cleared.")


# ============================
# Mailbox Polling (IMAP)
# ============================
st.markdown("### Mailbox Polling")
IMAP_HOST = st.secrets.get("IMAP_HOST")
IMAP_USER = st.secrets.get("IMAP_USER")
IMAP_PASS = st.secrets.get("IMAP_PASS")
IMAP_FOLDER = st.secrets.get("IMAP_FOLDER", "INBOX")
IMAP_SENDER = st.secrets.get("IMAP_SENDER")  # e.g., "alerts@flightaware.com"

enable_poll = st.checkbox("Enable IMAP polling", value=False,
                          help="Poll the mailbox for FlightAware alerts and auto-apply updates.")

def choose_booking_for_event(event: str, raw_from: str | None, raw_to: str | None, raw_at: str | None,
                             tails: list[str], actual_dt_utc: datetime) -> str | None:
    """Pick the most likely booking based on tail & time proximity (+ soft IATA/ICAO hints)."""
    cand = df_clean[df_clean["Aircraft"].isin(tails)].copy() if tails else df_clean.copy()

    def match_token(cdf, col_iata, col_icao, token):
        if not token:
            return cdf
        tok_iata = normalize_iata(token)
        tok_icao = token if len(token) == 4 else None
        if tok_iata and tok_icao:
            mask = (cdf[col_iata] == tok_iata) | (cdf[col_icao] == tok_icao)
        elif tok_iata:
            mask = (cdf[col_iata] == tok_iata)
        elif tok_icao:
            mask = (cdf[col_icao] == tok_icao)
        else:
            return cdf
        filtered = cdf[mask]
        return filtered if not filtered.empty else cdf

    if event in ("Arrival", "ArrivalForecast"):
        if raw_at:
            cand = match_token(cand, "To (IATA)" if has_iata else "To (ICAO)", "To (ICAO)", raw_at)
        if raw_from:
            cand = match_token(cand, "From (IATA)" if has_iata else "From (ICAO)", "From (ICAO)", raw_from)
        sched_col = "ETA_UTC"
    elif event in ("Departure", "EDCT"):
        if raw_from:
            cand = match_token(cand, "From (IATA)" if has_iata else "From (ICAO)", "From (ICAO)", raw_from)
        if raw_to:
            cand = match_token(cand, "To (IATA)" if has_iata else "To (ICAO)", "To (ICAO)", raw_to)
        sched_col = "ETD_UTC"
    elif event == "Diversion":
        if raw_from:
            cand = match_token(cand, "From (IATA)" if has_iata else "From (ICAO)", "From (ICAO)", raw_from)
        sched_col = "ETA_UTC"   # 👈 diversion vs ETA
    else:
        if raw_from:
            cand = match_token(cand, "From (IATA)" if has_iata else "From (ICAO)", "From (ICAO)", raw_from)
        sched_col = "ETD_UTC"

    if cand.empty:
        return None

    cand["Δ"] = (cand[sched_col] - actual_dt_utc).abs()
    cand = cand.sort_values("Δ")
    MAX_WINDOW = pd.Timedelta(hours=12) if event == "Diversion" else pd.Timedelta(hours=3)
    if not cand.empty and cand.iloc[0]["Δ"] <= MAX_WINDOW:
        return cand.iloc[0]["Booking"]
    return None

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

    # Search
    if IMAP_SENDER:
        typ, data = M.uid('search', None, 'FROM', f'"{IMAP_SENDER}"')
    else:
        typ, data = M.uid('search', None, 'ALL')
    if typ != "OK":
        st.error("IMAP search failed")
        M.logout()
        return 0

    uids = [int(x) for x in (data[0].split() if data and data[0] else [])]
    if not uids:
        M.logout()
        return 0

    last_uid = get_last_uid(IMAP_USER + ":" + IMAP_FOLDER)
    new_uids = [u for u in uids if u > last_uid]
    if not new_uids:
        M.logout()
        return 0

    applied = 0
    for uid in sorted(new_uids)[:max_to_process]:
        typ, msg_data = M.uid('fetch', str(uid), '(RFC822)')
        if typ != "OK" or not msg_data or not msg_data[0]:
            set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
            continue
        try:
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)
            subject = msg.get('Subject', '')
            sent_dt = msg.get('Date')
            try:
                sent_dt_utc = dateparse.parse(sent_dt)
                if sent_dt_utc.tzinfo is None:
                    sent_dt_utc = sent_dt_utc.replace(tzinfo=timezone.utc)
                sent_dt_utc = sent_dt_utc.astimezone(timezone.utc)
            except Exception:
                sent_dt_utc = datetime.now(timezone.utc)

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

            # Body cues: "departed ... at 02:18PM MDT", "arrived ... at 04:44PM MDT", "estimated arrival at 04:31PM MDT"
            body_dep = re.search(r"\bdepart(?:ed|ure)\b.*?\bat\s+(\d{1,2}:\d{2}\s*[APap][Mm])\s*([A-Z]{2,4})", body, re.I)
            body_arr = re.search(r"\barrived\b.*?\bat\s+(\d{1,2}:\d{2}\s*[APap][Mm])\s*([A-Z]{2,4})", body, re.I)
            body_eta = re.search(r"\bestimated\s+arrival\s+at\s+(\d{1,2}:\d{2}\s*[APap][Mm])\s*([A-Z]{2,4})", body, re.I)

            body_dep_dt = parse_body_time_to_utc("", body_dep.group(1), body_dep.group(2), sent_dt_utc) if body_dep else None
            body_arr_dt = parse_body_time_to_utc("", body_arr.group(1), body_arr.group(2), sent_dt_utc) if body_arr else None
            body_eta_dt = parse_body_time_to_utc("", body_eta.group(1), body_eta.group(2), sent_dt_utc) if body_eta else None

            # Fallbacks
            if not event:
                event = extract_event(text)
            if actual_dt_utc is None:
                # Prefer body explicit times, else parse any datetime, else 'Date' header
                actual_dt_utc = body_dep_dt or body_arr_dt or parse_any_datetime_to_utc(text) or sent_dt_utc

            # Airports from subject tokens (soft hints)
            raw_from = subj_info.get("from_airport")
            raw_to   = subj_info.get("to_airport")
            raw_at   = subj_info.get("at_airport")

            # Booking candidates
            bookings = extract_candidates(text)[0]
            booking = bookings[0] if bookings else None

            # Tails (normalize hyphen)
            tails = []
            if subj_info.get("tail"): tails.append(normalize_tail_token(subj_info["tail"]))
            tails += [normalize_tail_token(x) for x in re.findall(r"\bC-?[A-Z0-9]{4}\b", text)]
            tails = sorted(set(tails))

            if not booking:
                booking = choose_booking_for_event(event or "", raw_from, raw_to, raw_at, tails, actual_dt_utc)

            if not (booking and event and actual_dt_utc):
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                continue

            row = df_clean[df_clean["Booking"] == booking]
            planned = None
            if not row.empty:
                planned = row["ETA_UTC"].iloc[0] if event in ("Arrival", "ArrivalForecast", "Diversion") else row["ETD_UTC"].iloc[0]
            delta_min = None
            if planned is not None and pd.notna(planned):
                delta_min = int(round((actual_dt_utc - planned).total_seconds() / 60.0))

            if event == "Diversion":
                status = f"🔷 DIVERTED to {subj_info.get('to_airport','—')}"
            elif event == "ArrivalForecast":
                status = "🟦 ARRIVING SOON"
            elif event == "Arrival":
                status = "🟣 ARRIVED" if (delta_min is None or abs(delta_min) < int(delay_threshold_min)) else "🟠 LATE ARRIVAL"
            elif event == "EDCT":
                status = "🟪 EDCT"
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

            # Persist ETA from any email (EDCT or body)
            eta_any = body_eta_dt
            if eta_any:
                upsert_status(booking, "ArrivalForecast", "🟦 ARRIVING SOON", eta_any.isoformat(), None)

            # If body had explicit arrival/dep times, persist as well (more accurate than subject ts)
            if body_dep_dt and event != "Departure":
                upsert_status(booking, "Departure", "🟢 DEPARTED", body_dep_dt.isoformat(), None)
            if body_arr_dt and event != "Arrival":
                upsert_status(booking, "Arrival", "🟣 ARRIVED", body_arr_dt.isoformat(), None)

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
