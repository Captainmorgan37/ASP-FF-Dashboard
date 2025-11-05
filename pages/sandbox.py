import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

# -------------------------------------------------------------------
# Mock flight data (replace with your live data)
# -------------------------------------------------------------------
data = [
    {"Aircraft": "C-FSNY", "From": "CYEG", "To": "CYYC", "ETA": "2025-11-05 20:15", "Status": "Delayed"},
    {"Aircraft": "C-FASY", "From": "CYYC", "To": "CYLW", "ETA": "2025-11-05 22:10", "Status": "On Time"},
    {"Aircraft": "C-GZAS", "From": "CYYC", "To": "CYVR", "ETA": "2025-11-05 23:45", "Status": "Late"},
]
df = pd.DataFrame(data)

# -------------------------------------------------------------------
# Convert aircraft values into HTML hyperlinks
# -------------------------------------------------------------------
df["Aircraft"] = df["Aircraft"].apply(
    lambda x: f'<a href="https://www.flightaware.com/live/flight/{x.replace("-", "")}" target="_blank" style="color:#4da6ff;text-decoration:none;">{x}</a>'
)

# -------------------------------------------------------------------
# Display using custom HTML (preserves formatting + adds clickable links)
# -------------------------------------------------------------------
st.markdown("### ✈️ Flight-Following Dashboard")

# Optional styling tweak (dark theme link colors)
st.markdown("""
    <style>
    table {
        border-collapse: collapse;
        width: 100%;
    }
    th, td {
        text-align: left;
        padding: 6px 10px;
    }
    th {
        background-color: #262730;
        color: #f5f5f5;
    }
    tr:nth-child(even) { background-color: #1e1e1e; }
    tr:nth-child(odd) { background-color: #2a2a2a; }
    td {
        color: #f5f5f5;
    }
    a {
        color: #4da6ff;
        text-decoration: none;
        font-weight: 500;
    }
    a:hover {
        text-decoration: underline;
    }
    </style>
""", unsafe_allow_html=True)

# Render the DataFrame as HTML safely
st.markdown(df.to_html(escape=False, index=False), unsafe_allow_html=True)
