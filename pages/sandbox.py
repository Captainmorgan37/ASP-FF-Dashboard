import streamlit as st
import pandas as pd

st.markdown("### ✈️ Flight-Following Dashboard – Interactive View")

# -------------------------------------------------------------------
# Mock flight data – replace with your actual dashboard dataframe
# -------------------------------------------------------------------
data = [
    {"Aircraft": "C-FSNY", "From": "CYEG", "To": "CYYC",
     "ETA (UTC)": "2025-11-05 20:15", "Δ (min)": 22,
     "Status": "Delayed", "Booking": "WUOKE"},
    {"Aircraft": "C-FASY", "From": "CYYC", "To": "CYLW",
     "ETA (UTC)": "2025-11-05 22:10", "Δ (min)": 0,
     "Status": "On Time", "Booking": "ECYUO"},
    {"Aircraft": "C-GZAS", "From": "CYYC", "To": "CYVR",
     "ETA (UTC)": "2025-11-05 23:45", "Δ (min)": 9,
     "Status": "Late", "Booking": "ERQYP"},
]
frame = pd.DataFrame(data)

# -------------------------------------------------------------------
# Function to make FlightAware hyperlinks
# -------------------------------------------------------------------
def make_fa_link(ac):
    if not ac or ac == "—":
        return ac
    return f"[{ac}](https://www.flightaware.com/live/flight/{ac.replace('-', '')})"

# -------------------------------------------------------------------
# Header row
# -------------------------------------------------------------------
header = st.columns([1.3, 1, 1, 1.3, 0.8, 1.4])
for h, col in zip(
    ["Aircraft", "From", "To", "ETA (UTC)", "Δ (min)", "Status / Action"], header
):
    col.markdown(f"**{h}**", unsafe_allow_html=True)

st.markdown("<hr style='margin:4px 0;'>", unsafe_allow_html=True)

# -------------------------------------------------------------------
# Loop rows
# -------------------------------------------------------------------
for idx, row in frame.iterrows():
    cols = st.columns([1.3, 1, 1, 1.3, 0.8, 1.4])
    # clickable aircraft link
    cols[0].markdown(make_fa_link(row["Aircraft"]), unsafe_allow_html=True)
    cols[1].markdown(row["From"])
    cols[2].markdown(row["To"])
    cols[3].markdown(row["ETA (UTC)"])
    cols[4].markdown(str(row["Δ (min)"]))

    status = str(row["Status"]).lower()
    if status in ("delayed", "late", "atc delay"):
        with cols[5].form(f"delay_form_{idx}", clear_on_submit=True):
            reason = st.text_input("", placeholder="Reason (optional)",
                                   key=f"reason_{idx}")
            send = st.form_submit_button("📣 Post to TELUS")
            if send:
                reason_to_send = reason.strip() or "Unknown"
                # 🔗 call your webhook here
                # notify_delay_chat(team="FF", tail=row["Aircraft"], ...)
                st.success(f"Posted: {row['Aircraft']} ({reason_to_send})")
    else:
        cols[5].markdown(row["Status"])

    st.markdown("<hr style='margin:2px 0;'>", unsafe_allow_html=True)
