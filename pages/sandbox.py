import streamlit as st
import pandas as pd

# Mock data (replace with your live data)
data = [
    {"Aircraft": "C-FSNY", "From": "CYEG", "To": "CYYC", "ETA": "2025-11-05 20:15", "Status": "Delayed"},
    {"Aircraft": "C-FASY", "From": "CYYC", "To": "CYLW", "ETA": "2025-11-05 22:10", "Status": "On Time"},
    {"Aircraft": "C-GZAS", "From": "CYYC", "To": "CYVR", "ETA": "2025-11-05 23:45", "Status": "Late"},
]
df = pd.DataFrame(data)

# ✅ Convert Aircraft column to Markdown hyperlinks
df["Aircraft"] = df["Aircraft"].apply(
    lambda x: f"[{x}](https://www.flightaware.com/live/flight/{x.replace('-', '')})"
)

# Display the table with Markdown rendering
st.markdown("### ✈️ Flight-Following Dashboard")

# Streamlit 1.29+ supports Markdown links in st.dataframe if use_container_width=True
st.dataframe(df, use_container_width=True)
