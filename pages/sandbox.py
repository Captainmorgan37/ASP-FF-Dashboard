import streamlit as st
import pandas as pd
from datetime import datetime

# -------------------------------------------------------------------
# Mock data – replace with your live dashboard DataFrame
# -------------------------------------------------------------------
data = [
    {"Aircraft": "C-FSNY", "From": "CYEG", "To": "CYYC", "ETA (UTC)": "2025-11-05 20:15",
     "Δ (min)": 22, "Status": "Delayed", "Booking": "WUOKE"},
    {"Aircraft": "C-FASY", "From": "CYYC", "To": "CYLW", "ETA (UTC)": "2025-11-05 22:10",
     "Δ (min)": 0,  "Status": "On Time", "Booking": "ECYUO"},
    {"Aircraft": "C-GZAS", "From": "CYYC", "To": "CYVR", "ETA (UTC)": "2025-11-05 23:45",
     "Δ (min)": 9,  "Status": "Late",    "Booking": "ERQYP"},
]
frame = pd.DataFrame(data)

# -------------------------------------------------------------------
# 1️⃣  Aircraft column → clickable FlightAware link (same visual look)
# -------------------------------------------------------------------
def make_fa_link(ac):
    if not ac or ac == "—":
        return ac
    url = f"https://www.flightaware.com/live/flight/{ac.replace('-', '')}"
    return f"[{ac}]({url})"

frame["Aircraft"] = frame["Aircraft"].apply(make_fa_link)

# -------------------------------------------------------------------
# 2️⃣  Table header styling
# -------------------------------------------------------------------
st.markdown("### ✈️ Flight-Following Dashboard – Interactive View")
st.markdown("""
<style>
table.dashboard {width:100%; border-collapse:collapse;}
table.dashboard th, table.dashboard td {
  border:1px solid #444;
  padding:6px 10px;
  text-align:center;
}
table.dashboard th {background:#222; color:#ccc;}
table.dashboard td a {text-decoration:none; color:#4da6ff;}
table.dashboard td button {
  background:#444; border:none; border-radius:4px;
  color:white; padding:2px 8px; cursor:pointer;
}
table.dashboard td button:hover {background:#ff8c00;}
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------------------
# 3️⃣  Build table rows with inline “Post to TELUS” button
# -------------------------------------------------------------------
rows_html = """
<table class='dashboard'>
  <tr>
    <th>Aircraft</th><th>From</th><th>To</th>
    <th>ETA (UTC)</th><th>Δ (min)</th><th>Status / Action</th>
  </tr>
"""

for i, row in frame.iterrows():
    # Button only for delayed-type statuses
    is_delay = str(row["Status"]).lower() in ("delayed", "late", "atc delay")
    btn_html = ""
    if is_delay:
        # create a unique form key per row
        with st.form(f"form_{i}", clear_on_submit=True):
            cols = st.columns([0.75, 1])
            reason = cols[0].text_input(
                f"Delay reason ({row['Aircraft']})",
                placeholder="optional"
            )
            send = cols[1].form_submit_button("📣 Post to TELUS")
            if send:
                reason_to_send = reason.strip() or "Unknown"
                # 🔗 call your real webhook here – example:
                # notify_delay_chat(team="FF", tail=row["Aircraft"],
                #     booking=row["Booking"], minutes_delta=row["Δ (min)"],
                #     new_eta_hhmm=row["ETA (UTC)"], reason=reason_to_send)
                st.success(f"Posted → {row['Aircraft']} ({reason_to_send})")
        btn_html = ""  # placeholder; actual form already rendered
    else:
        btn_html = row["Status"]

    rows_html += f"""
    <tr>
      <td>{row['Aircraft']}</td>
      <td>{row['From']}</td>
      <td>{row['To']}</td>
      <td>{row['ETA (UTC)']}</td>
      <td>{row['Δ (min)']}</td>
      <td>{btn_html if not is_delay else ''}</td>
    </tr>
    """

rows_html += "</table>"
st.markdown(rows_html, unsafe_allow_html=True)
