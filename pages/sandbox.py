import streamlit as st
import pandas as pd
from datetime import datetime

# -------------------------------------------------------------------
# Mock data – replace with your actual 'frame' from the dashboard
# -------------------------------------------------------------------
data = [
    {"Aircraft": "C-FSNY", "From": "CYEG", "To": "CYYC", "ETA (UTC)": "2025-11-05 20:15",
     "Δ (min)": 22, "Status": "Delayed", "Booking": "WUOKE"},
    {"Aircraft": "C-FASY", "From": "CYYC", "To": "CYLW", "ETA (UTC)": "2025-11-05 22:10",
     "Δ (min)": 0, "Status": "On Time", "Booking": "ECYUO"},
    {"Aircraft": "C-GZAS", "From": "CYYC", "To": "CYVR", "ETA (UTC)": "2025-11-05 23:45",
     "Δ (min)": 9, "Status": "Late", "Booking": "ERQYP"},
]
frame = pd.DataFrame(data)

# -------------------------------------------------------------------
# 1️⃣  Make Aircraft column clickable FlightAware links (same look)
# -------------------------------------------------------------------
def make_fa_link(ac):
    if not ac or ac == "—":
        return ac
    url = f"https://www.flightaware.com/live/flight/{ac.replace('-', '')}"
    return f"[{ac}]({url})"

frame["Aircraft"] = frame["Aircraft"].apply(make_fa_link)

# -------------------------------------------------------------------
# 2️⃣  Build table row by row so we can insert buttons conditionally
# -------------------------------------------------------------------
st.markdown("### ✈️ Flight-Following Dashboard – Interactive Prototype")
st.markdown(
    "<style>table {width:100%; border-collapse:collapse;} th,td {padding:6px 10px;}</style>",
    unsafe_allow_html=True
)

for idx, row in frame.iterrows():
    with st.container(border=True):
        cols = st.columns([1.2, 1, 1, 1.3, 0.8, 1.2])
        cols[0].markdown(row["Aircraft"], unsafe_allow_html=True)
        cols[1].write(row["From"])
        cols[2].write(row["To"])
        cols[3].write(row["ETA (UTC)"])
        cols[4].write(row["Δ (min)"])
        cols[5].write(row["Status"])

        # -------------------------------------------------------------------
        # 3️⃣  Conditional "Post to TELUS" button for delayed statuses
        # -------------------------------------------------------------------
        if str(row["Status"]).lower() in ("delayed", "late", "atc delay"):
            btn_key = f"post_{idx}"
            if st.button("📣 Post to TELUS", key=btn_key):
                st.session_state["pending_post"] = row.to_dict()

# -------------------------------------------------------------------
# 4️⃣  Modal popup for reason entry & webhook posting
# -------------------------------------------------------------------
if "pending_post" in st.session_state:
    flight = st.session_state["pending_post"]

    with st.modal(f"Post delay – {flight['Aircraft'].strip('[]()')}"):
        st.write(f"**Route:** {flight['From']} ➜ {flight['To']}")
        st.write(f"**Status:** {flight['Status']}")
        reason = st.text_input("Delay reason (optional)")
        colA, colB = st.columns(2)
        if colA.button("✅ Send"):
            reason_to_send = reason if reason.strip() else "Unknown"

            # 🔗 Call your existing webhook here ----------------------------
            # Example:
            # notify_delay_chat(
            #     team="FF",
            #     tail=flight["Aircraft"],
            #     booking=flight["Booking"],
            #     minutes_delta=flight["Δ (min)"],
            #     new_eta_hhmm=flight["ETA (UTC)"],
            #     reason=reason_to_send
            # )
            st.success(f"Posted to TELUS – Reason: {reason_to_send}")
            del st.session_state["pending_post"]

        if colB.button("Cancel"):
            del st.session_state["pending_post"]
