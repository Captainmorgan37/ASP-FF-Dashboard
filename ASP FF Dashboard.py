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
from dateutil.tz import tzoffset

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
            event_type TEXT NOT NULL,  -- 'Departure' | 'Arrival' | 'ArrivalForecast' | 'Diversion' | 'EDCT'
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
TZINFOS = {
    "UTC":  tzoffset("UTC", 0),
    "GMT":  tzoffset("GMT", 0),

    "AST":  tzoffset("AST",  -4*3600),
    "ADT":  tzoffset("ADT",  -3*3600),
    "EST":  tzoffset("EST",  -5*3600),
    "EDT":  tzoffset("EDT",  -4*3600),
    "CST":  tzoffset("CST",  -6*3600),
    "CDT":  tzoffset("CDT",  -5*3600),
    "MST":  tzoffset("MST",  -7*3600),
    "MDT":  tzoffset("MDT",  -6*3600),
    "PST":  tzoffset("PST",  -8*3600),
    "PDT":  tzoffset("PDT",  -7*3600),

    "AKST": tzoffset("AKST", -9*3600),
    "AKDT": tzoffset("AKDT", -8*3600),
    "HST":  tzoffset("HST", -10*3600),

    "NST":  tzoffset("NST",  -(3*3600 + 1800)),
    "NDT":  tzoffset("NDT",  -(2*3600 + 1800)),
}

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

# REPLACE your existing utc_datetime_picker with this:

def utc_datetime_picker(label: str, key: str, initial_dt_utc: datetime | None = None) -> datetime:
    """Stateful UTC datetime picker (persists across reruns/auto-refresh)."""
    if initial_dt_utc is None:
        initial_dt_utc = datetime.now(timezone.utc)

    date_key = f"{key}__date"
    time_key = f"{key}__time"

    # Seed once (only if not already set)
    if date_key not in st.session_state:
        st.session_state[date_key] = initial_dt_utc.date()
    if time_key not in st.session_state:
        st.session_state[time_key] = initial_dt_utc.time().replace(microsecond=0)

    # Use session state as the single source of truth (no dynamic 'value=')
    d = st.date_input(f"{label} — Date (UTC)", key=date_key)
    t = st.time_input(f"{label} — Time (UTC)", key=time_key)

    return datetime.combine(d, t).replace(tzinfo=timezone.utc)



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
    m_iso = re.search(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(Z|[+\-]\d{2}:?\d{2})?", text)
    if m_iso:
        try:
            dt = dateparse.parse(m_iso.group(0), tzinfos=TZINFOS)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
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
            dt = dateparse.parse(s, fuzzy=True, tzinfos=TZINFOS)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None

def parse_any_dt_string_to_utc(s: str) -> datetime | None:
    if not s:
        return None
    try:
        dt = dateparse.parse(s, fuzzy=True, tzinfos=TZINFOS)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
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
    if re.search(r"\bEDCT\b|Expected Departure Clearance Time", text, re.I):
        return "EDCT"
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

# ---- BODY parsers ----
BODY_DEPARTURE_RE = re.compile(
    r"departed\s+.*?\((?P<from>[A-Z]{3,4})\)\s+at\s+"
    r"(?P<dep_time>\d{1,2}:\d{2}(?:\s*[AP]M)?(?:\s*[A-Z]{2,4})?)"
    r".*?en\s*route\s+to\s+.*?\((?P<to>[A-Z]{3,4})\)"
    r".*?(?:(?:(?:estimated\s+(?:time\s+of\s+)?arrival|ETA)\s*(?:at|of)?\s+"
    r"(?P<eta_time>\d{1,2}:\d{2}(?:\s*[AP]M)?(?:\s*[A-Z]{2,4})?)))?",
    re.I
)
BODY_ARRIVAL_RE = re.compile(
    r"arrived\s+at\s+.*?\((?P<at>[A-Z]{3,4})\)\s+at\s+(?P<arr_time>\d{1,2}:\d{2}\s*[A-Z0-9 ]+)"
    r".*?from\s+.*?\((?P<from>[A-Z]{3,4})\)",
    re.I
)
ETA_ANY_RE = re.compile(
    r"(?:estimated\s+(?:time\s+of\s+)?arrival|ETA)\s*(?:at|of)?\s+"
    r"(\d{1,2}:\d{2}(?:\s*[AP]M)?(?:\s*[A-Z]{2,4})?)",
    re.I
)

# ---- EDCT email parsers ----
EDCT_FROM_TO_RE = re.compile(
    r"your flight from\s+(?P<from>[A-Z]{3,4})\s+to\s+(?P<to>[A-Z]{3,4})",
    re.I
)
EDCT_EDCT_RE = re.compile(r"^\s*EDCT:\s*(?P<edct_line>.+)$", re.I | re.M)
EDCT_EXP_ARR_RE = re.compile(r"^\s*Expected Arrival Time:\s*(?P<eta_line>.+)$", re.I | re.M)
EDCT_ORIG_DEP_RE = re.compile(r"^\s*Original Departure Time:\s*(?P<orig_line>.+)$", re.I | re.M)

def _parse_time_token_to_utc(time_token: str, base_date_utc: datetime) -> datetime | None:
    if not time_token:
        return None
    base_day = (base_date_utc or datetime.now(timezone.utc)).date()
    s = f"{base_day.isoformat()} {time_token}"
    try:
        dt = dateparse.parse(s, fuzzy=True, tzinfos=TZINFOS)
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
        if not info.get("eta_time_utc"):
            m_eta = ETA_ANY_RE.search(body)
            if m_eta:
                info["eta_time_utc"] = _parse_time_token_to_utc(m_eta.group(1), email_date_utc)
        return info
    m = BODY_ARRIVAL_RE.search(body)
    if event == "Arrival" and m:
        info["at"] = m.group("at")
        info["from"] = m.group("from")
        info["arr_time_utc"] = _parse_time_token_to_utc(m.group("arr_time"), email_date_utc)
        return info
    # If the event wasn't detected as "Departure", try to capture an ETA anywhere.
    m_eta = ETA_ANY_RE.search(body)
    if m_eta:
        info["eta_time_utc"] = _parse_time_token_to_utc(m_eta.group(1), email_date_utc)
    return info

def parse_body_edct(body: str) -> dict:
    """Return {'from','to','edct_time_utc','expected_arrival_utc','original_dep_utc'} if present."""
    info = {}
    if not body:
        return info
    mft = EDCT_FROM_TO_RE.search(body)
    if mft:
        info["from"] = mft.group("from").upper()
        info["to"] = mft.group("to").upper()
    m_edct = EDCT_EDCT_RE.search(body)
    if m_edct:
        info["edct_time_utc"] = parse_any_dt_string_to_utc(m_edct.group("edct_line"))
    m_eta = EDCT_EXP_ARR_RE.search(body)
    if m_eta:
        info["expected_arrival_utc"] = parse_any_dt_string_to_utc(m_eta.group("eta_line"))
    m_orig = EDCT_ORIG_DEP_RE.search(body)
    if m_orig:
        info["original_dep_utc"] = parse_any_dt_string_to_utc(m_orig.group("orig_line"))
    return info

# ============================
# CSV IATA/ICAO support
# ============================
def normalize_iata(code: str) -> str:
    c = (code or "").strip().upper()
    return c if len(c) == 3 else ""

def derive_iata_from_icao(icao: str) -> str:
    c = (icao or "").strip().upper()
    if len(c) == 4 and c[0] in ("C", "K"):
        return c[1:]
    return ""

def display_airport(icao: str, iata: str) -> str:
    i = (icao or "").strip().upper()
    a = (iata or "").strip().upper()
    if i and len(i) == 4:
        return i
    if a and len(a) == 3:
        return a
    return "—"

# ---- Booking chooser ----
def choose_booking_for_event(subj_info: dict, tails: list[str], event: str, event_dt_utc: datetime) -> str | None:
    if event_dt_utc is None or event not in ("Arrival", "ArrivalForecast", "Departure", "Diversion", "EDCT"):
        return None
    cand = df_clean.copy()
    if tails:
        cand = cand[cand["Aircraft"].isin(tails)]
        if cand.empty:
            return None

    raw_at   = (subj_info.get("at_airport") or "").strip().upper()
    raw_from = (subj_info.get("from_airport") or "").strip().upper()
    raw_to   = (subj_info.get("to_airport") or "").strip().upper()

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
        return cdf[mask]

    if event in ("Arrival", "ArrivalForecast"):
        if raw_at:
            cand = match_token(cand, "To_IATA", "To_ICAO", raw_at)
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
        sched_col = "ETA_UTC"
    elif event in ("Departure", "EDCT"):
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
        if raw_to:
            cand = match_token(cand, "To_IATA", "To_ICAO", raw_to)
        sched_col = "ETD_UTC"
    else:
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
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
# File upload with persistence
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

# Priority order: upload → session → DB
if uploaded is not None:
    csv_bytes = uploaded.getvalue()
    st.session_state["csv_bytes"] = csv_bytes
    st.session_state["csv_name"] = uploaded.name
    st.session_state["csv_uploaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    save_csv_to_db(uploaded.name, csv_bytes)
    df_raw = _load_csv_from_bytes(csv_bytes)
elif "csv_bytes" in st.session_state:
    df_raw = _load_csv_from_bytes(st.session_state["csv_bytes"])
    st.caption(
        f"Using cached CSV: **{st.session_state.get('csv_name','flights.csv')}** "
        f"(uploaded {st.session_state.get('csv_uploaded_at','')})"
    )
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

df["From_ICAO"] = df["From (ICAO)"].astype(str).str.strip().str.upper().replace({"NAN": ""})
df["To_ICAO"]   = df["To (ICAO)"].astype(str).str.strip().str.upper().replace({"NAN": ""})

if "From (IATA)" in df_raw.columns:
    df["From_IATA"] = df_raw["From (IATA)"].astype(str).str.strip().str.upper()
else:
    df["From_IATA"] = df["From_ICAO"].apply(derive_iata_from_icao)

if "To (IATA)" in df_raw.columns:
    df["To_IATA"] = df_raw["To (IATA)"].astype(str).str.strip().str.upper()
else:
    df["To_IATA"] = df["To_ICAO"].apply(derive_iata_from_icao)

df["is_real_leg"] = df["Aircraft"].apply(is_real_tail)
df = df[df["is_real_leg"]].copy()

df["Type"] = df["Account"].apply(classify_account)
df["TypeBadge"] = df["Type"].apply(type_badge)

df["From"] = [display_airport(i, a) for i, a in zip(df["From_ICAO"], df["From_IATA"])]
df["To"]   = [display_airport(i, a) for i, a in zip(df["To_ICAO"], df["To_IATA"])]
df["Route"] = df["From"] + " → " + df["To"]

now_utc = datetime.now(timezone.utc)
df["Departs In"] = (df["ETD_UTC"] - now_utc).apply(fmt_td)
df["Arrives In"] = (df["ETA_UTC"] - now_utc).apply(fmt_td)

df_clean = df.copy()

# ============================
# Email-driven status + enrich FA/EDCT times
# ============================
events_map = load_status_map()
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

    if has_dep and has_div:
        return rec["Diversion"].get("status", "🔷 DIVERTED")
    if has_arr:
        return "🟣 ARRIVED"
    if has_dep and not has_arr:
        fore_eta = None
        if "ArrivalForecast" in rec:
            fore_eta = parse_iso_to_utc(rec["ArrivalForecast"].get("actual_time_utc"))
        eta_for_status = fore_eta or eta_utc
        if pd.notna(eta_for_status) and now > eta_for_status + thr:
            return "🟠 LATE ARRIVAL"
        return "🟢 DEPARTED"
    if pd.notna(dep_utc):
        return "🟡 SCHEDULED" if now <= dep_utc + thr else "🔴 DELAY"
    return "🟡 SCHEDULED"

df["Status"] = [compute_status_row(b, dep, eta) for b, dep, eta in zip(df["Booking"], df["ETD_UTC"], df["ETA_UTC"])]

# Pull persisted times
dep_actual_list, eta_fore_list, arr_actual_list, edct_list = [], [], [], []
for b in df["Booking"]:
    rec = events_map.get(b, {})
    dep_actual_list.append(parse_iso_to_utc(rec.get("Departure", {}).get("actual_time_utc")))
    eta_fore_list.append(parse_iso_to_utc(rec.get("ArrivalForecast", {}).get("actual_time_utc")))
    arr_actual_list.append(parse_iso_to_utc(rec.get("Arrival", {}).get("actual_time_utc")))
    edct_list.append(parse_iso_to_utc(rec.get("EDCT", {}).get("actual_time_utc")))

# Display columns
# Off-Block (Actual): show EDCT (purple) until a true Departure arrives, then overwrite with actual time
offblock_actual_display = []
for dep_dt, edct_dt in zip(dep_actual_list, edct_list):
    if dep_dt:
        offblock_actual_display.append(fmt_dt_utc(dep_dt))
    elif edct_dt:
        offblock_actual_display.append(f"EDCT - {fmt_dt_utc(edct_dt)}")
    else:
        offblock_actual_display.append("—")

df["Off-Block (Actual)"] = offblock_actual_display
df["ETA (FA)"]           = [fmt_dt_utc(x) for x in eta_fore_list]
df["On-Block (Actual)"]  = [fmt_dt_utc(x) for x in arr_actual_list]

# Hidden raw timestamps for styling/calcs (do NOT treat EDCT as actual)
df["_DepActual_ts"] = pd.to_datetime(dep_actual_list, utc=True)     # True actual OUT only
df["_ETA_FA_ts"]    = pd.to_datetime(eta_fore_list,   utc=True)
df["_ArrActual_ts"] = pd.to_datetime(arr_actual_list, utc=True)
df["_EDCT_ts"]      = pd.to_datetime(edct_list,       utc=True)

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

# ============================
# Sort, compute row/cell highlights, display
# ============================
df = df.sort_values(by=["ETD_UTC", "ETA_UTC"], ascending=[True, True]).copy()

delay_thr_td    = pd.Timedelta(minutes=int(delay_threshold_min))   # e.g., 15m
row_red_thr_td  = pd.Timedelta(minutes=max(30, int(delay_threshold_min)))  # ≥30m

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

# Cell-level red accents (do not count EDCT as actual departure)
dep_delay        = df["_DepActual_ts"] - df["ETD_UTC"]   # Off-Block (Actual true) - Off-Block (Est)
eta_fa_vs_sched  = df["_ETA_FA_ts"]    - df["ETA_UTC"]   # ETA (FA) - On-Block (Est)
arr_vs_sched     = df["_ArrActual_ts"] - df["ETA_UTC"]   # On-Block (Actual) - On-Block (Est)

cell_dep = dep_delay.notna()       & (dep_delay       > delay_thr_td)
cell_eta = eta_fa_vs_sched.notna() & (eta_fa_vs_sched > delay_thr_td)
cell_arr = arr_vs_sched.notna()    & (arr_vs_sched    > delay_thr_td)

display_cols = [
    "TypeBadge", "Booking", "Aircraft", "Aircraft Type", "Route",
    "Off-Block (Est)", "Off-Block (Actual)", "ETA (FA)",
    "On-Block (Est)", "On-Block (Actual)",
    "Departs In", "Arrives In",
    "PIC", "SIC", "Workflow", "Status"
]
df_display = df[display_cols].copy()

def _style_ops(x: pd.DataFrame):
    styles = pd.DataFrame("", index=x.index, columns=x.columns)

    # Row backgrounds (yellow/red)
    row_y_css = "background-color: rgba(255, 193, 7, 0.18); border-left: 6px solid #ffc107;"
    row_r_css = "background-color: rgba(255, 82, 82, 0.18); border-left: 6px solid #ff5252;"
    styles.loc[row_yellow.reindex(x.index, fill_value=False), :] = row_y_css
    styles.loc[row_red.reindex(x.index,    fill_value=False), :] = row_r_css  # red overrides yellow

    # Cell-only red accents for variance rules
    cell_css = "background-color: rgba(255, 82, 82, 0.25);"

    idx = cell_dep.reindex(x.index, fill_value=False)
    styles.loc[idx, "Off-Block (Actual)"] += cell_css

    idx = cell_eta.reindex(x.index, fill_value=False)
    styles.loc[idx, "ETA (FA)"] += cell_css

    idx = cell_arr.reindex(x.index, fill_value=False)
    styles.loc[idx, "On-Block (Actual)"] += cell_css

    # EDCT: purple cell on Off-Block (Actual) when EDCT exists & no true departure yet.
    cell_edct_css = "background-color: rgba(155, 81, 224, 0.28); border-left: 6px solid #9b51e0;"
    idx_edct = (df["_EDCT_ts"].notna() & df["_DepActual_ts"].isna()).reindex(x.index, fill_value=False)
    # Apply AFTER row styles so purple wins even if the row is red.
    styles.loc[idx_edct, "Off-Block (Actual)"] += cell_edct_css

    return styles

st.subheader(f"Schedule  ·  {len(df_display)} flight(s) shown")
st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")
st.dataframe(
    df_display.style.hide(axis="index").apply(_style_ops, axis=None),
    use_container_width=True
)
st.caption(
    "Row colors (operational): **yellow** = 15–29 min late without matching email, **red** = ≥30 min late. "
    "Cell accents: red = variance (Off-Block Actual>Est, ETA(FA)>On-Block Est, On-Block Actual>Est). "
    "When an EDCT email arrives, **Off-Block (Actual)** shows “EDCT - …” in purple until a Departure email is received."
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

        override_dep_dt = None
        override_arr_dt = None
        if set_dep:
            dep_seed = (row["ETD_UTC"].to_pydatetime() if pd.notna(row["ETD_UTC"]) else datetime.now(timezone.utc))
            override_dep_dt = utc_datetime_picker("Actual Departure (UTC)", key="override_dep", initial_dt_utc=dep_seed)
        
        if set_arr:
            arr_seed = (row["ETA_UTC"].to_pydatetime() if pd.notna(row["ETA_UTC"]) else datetime.now(timezone.utc))
            override_arr_dt = utc_datetime_picker("Actual Arrival (UTC)", key="override_arr", initial_dt_utc=arr_seed)


        cbtn1, cbtn2, cbtn3 = st.columns([1,1,2])
        with cbtn1:
            if st.button("Save override(s)"):
                st.session_state.setdefault("status_updates", {})

                if set_dep and override_dep_dt:
                    # Compute delta vs scheduled ETD (for reference)
                    delta_min = None
                    if pd.notna(planned_dep):
                        delta_min = int(round((override_dep_dt - planned_dep).total_seconds() / 60.0))

                    # Persist as a normal Departure event (board will treat it like an email)
                    upsert_status(sel_booking, "Departure", "🟢 DEPARTED", override_dep_dt.isoformat(), delta_min)
                    st.session_state["status_updates"][sel_booking] = {
                        **st.session_state["status_updates"].get(sel_booking, {}),
                        "type": "Departure",
                        "actual_time_utc": override_dep_dt.isoformat(),
                        "delta_min": delta_min,
                        "status": "🟢 DEPARTED",
                    }

                if set_arr and override_arr_dt:
                    # Compute delta vs scheduled ETA (for reference)
                    delta_min = None
                    if pd.notna(planned_arr):
                        delta_min = int(round((override_arr_dt - planned_arr).total_seconds() / 60.0))

                    # Persist as a normal Arrival event
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
                # Clear whichever boxes are checked
                if set_dep:
                    delete_status(sel_booking, "Departure")
                    # Remove from session_state if present
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
IMAP_SENDER = st.secrets.get("IMAP_SENDER")  # e.g., alerts@flightaware.com

if IMAP_SENDER:
    st.caption(f'IMAP filter: **From = {IMAP_SENDER}**')
else:
    st.caption("IMAP filter: **From = ALL senders**")

enable_poll = st.checkbox("Enable IMAP polling", value=False,
                          help="Poll the mailbox for FlightAware/FlightBridge alerts and auto-apply updates.")

def imap_poll_once(max_to_process: int = 25, debug: bool = False) -> int:
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

    last_uid = get_last_uid(IMAP_USER + ":" + IMAP_FOLDER)
    if IMAP_SENDER:
        typ, data = M.uid('search', None, 'FROM', f'"{IMAP_SENDER}"', f'UID {last_uid+1}:*')
        if typ != "OK" or not data or not data[0]:
            if debug:
                st.warning(f'No matches for FROM filter "{IMAP_SENDER}". Falling back to unfiltered UID search.')
            typ, data = M.uid('search', None, f'UID {last_uid+1}:*')
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

            # Times / body parses
            hdr_dt = get_email_date_utc(msg)
            explicit_dt = parse_any_datetime_to_utc(text)
            body_info = parse_body_firstline(event, body, hdr_dt or now_utc)
            edct_info = parse_body_edct(body)

            # If nothing matched but looks like EDCT, set event
            if not event and (edct_info.get("edct_time_utc") or re.search(r"\bEDCT\b|Expected Departure Clearance Time", text, re.I)):
                event = "EDCT"

            # Prefer EDCT/Body airports when subject lacks them
            if edct_info.get("from") and not subj_info.get("from_airport"):
                subj_info["from_airport"] = edct_info["from"]
            if edct_info.get("to") and not subj_info.get("to_airport"):
                subj_info["to_airport"] = edct_info["to"]

            # Event time selection
            if event == "Departure":
                actual_dt_utc = body_info.get("dep_time_utc") or explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc
            elif event == "Arrival":
                actual_dt_utc = body_info.get("arr_time_utc") or explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc
            elif event == "EDCT":
                actual_dt_utc = edct_info.get("edct_time_utc") or explicit_dt or hdr_dt or now_utc
            else:
                actual_dt_utc = explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc

            # Booking selection
            bookings = extract_candidates(text)[0]
            booking = bookings[0] if bookings else None

            tails = []
            if subj_info.get("tail"): tails.append(subj_info["tail"])
            tails += re.findall(r"\bC-[A-Z0-9]{4}\b", text)
            tails = sorted(set(tails))

            if not booking:
                booking = choose_booking_for_event(subj_info, tails, event, actual_dt_utc)

            if not (booking and event and actual_dt_utc):
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                continue

            # Delta reference (not used for status)
            row = df_clean[df_clean["Booking"] == booking]
            planned = None
            if not row.empty:
                planned = row["ETA_UTC"].iloc[0] if event in ("Arrival", "ArrivalForecast") else row["ETD_UTC"].iloc[0]
            delta_min = None
            if planned is not None and pd.notna(planned):
                delta_min = int(round((actual_dt_utc - planned).total_seconds() / 60.0))

            # Canonical event -> status text
            if event == "Diversion":
                status = f"🔷 DIVERTED to {subj_info.get('to_airport','—')}"
            elif event == "ArrivalForecast":
                status = "🟦 ARRIVING SOON"
            elif event == "Arrival":
                status = "🟣 ARRIVED"
            elif event == "Departure":
                status = "🟢 DEPARTED"
            elif event == "EDCT":
                status = "🟪 EDCT"
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

            # Persist ArrivalForecast if present (from EDCT or Departure emails)
            if edct_info.get("expected_arrival_utc"):
                upsert_status(booking, "ArrivalForecast", "🟦 ARRIVING SOON", edct_info["expected_arrival_utc"].isoformat(), None)
            elif event == "Departure" and body_info.get("eta_time_utc"):
                upsert_status(booking, "ArrivalForecast", "🟦 ARRIVING SOON", body_info["eta_time_utc"].isoformat(), None)

            if debug:
                st.write({
                    "uid": uid,
                    "subject": subject,
                    "event": event,
                    "from_airport": subj_info.get("from_airport"),
                    "to_airport": subj_info.get("to_airport"),
                    "tails": tails,
                    "actual_dt_utc": actual_dt_utc.isoformat() if actual_dt_utc else None,
                    "eta_forecast": (edct_info.get("expected_arrival_utc") or body_info.get("eta_time_utc")).isoformat()
                                     if (edct_info.get("expected_arrival_utc") or body_info.get("eta_time_utc")) else None,
                    "booking": booking,
                })

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
        c_poll1, c_poll2, c_poll3 = st.columns([1,1,1])
        with c_poll1:
            debug_poll = st.checkbox("Debug IMAP (verbose)", value=False)
        with c_poll2:
            poll_on_refresh = st.checkbox("Poll automatically on refresh", value=True)
        with c_poll3:
            if st.button("Reset IMAP cursor only"):
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, 0)
                st.success("IMAP cursor reset to 0 (statuses preserved).")

        # Optional: control batch size if you expect backlogs
        max_per_poll = st.number_input("Max emails per poll", min_value=10, max_value=1000, value=200, step=10)

        if st.button("Poll now"):
            applied = imap_poll_once(max_to_process=int(max_per_poll), debug=debug_poll)
            st.success(f"Applied {applied} update(s) from mailbox.")

        if poll_on_refresh:
            try:
                applied = imap_poll_once(max_to_process=int(max_per_poll), debug=debug_poll)
                if applied:
                    st.info(f"Auto-poll applied {applied} update(s).")
            except Exception as e:
                st.error(f"Auto-poll error: {e}")
