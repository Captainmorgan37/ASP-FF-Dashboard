import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

st.set_page_config(layout="wide")

# -------------------------------------------------------------------
# Mock flight data (replace with your live FL3XX / FlightAware merge)
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
df = pd.DataFrame(data)

# -------------------------------------------------------------------
# JavaScript renderers for clickable links and button column
# -------------------------------------------------------------------

# Aircraft → FlightAware link
link_renderer = JsCode("""
function(params) {
  if (!params.value) return '';
  const url = 'https://www.flightaware.com/live/flight/' + params.value.replace('-', '');
  return `<a href="${url}" target="_blank" style="color:#4da6ff;text-decoration:none;">${params.value}</a>`;
}
""")

# Status coloring
status_style = JsCode("""
function(params){
  if (params.value == 'Delayed' || params.value == 'Late' || params.value == 'ATC Delay')
      return {'color':'#fff','backgroundColor':'#b33a3a'};
  if (params.value == 'On Time')
      return {'color':'#fff','backgroundColor':'#2d8031'};
  return {'color':'#ddd','backgroundColor':'#444'};
}
""")

# Action button
button_renderer = JsCode("""
class BtnCellRenderer {
  init(params){
    this.params = params;
    this.eGui = document.createElement('button');
    this.eGui.innerText = '📣 Post to TELUS';
    this.eGui.style.background = '#444';
    this.eGui.style.color = 'white';
    this.eGui.style.border = 'none';
    this.eGui.style.borderRadius = '4px';
    this.eGui.style.cursor = 'pointer';
    this.eGui.style.padding = '2px 8px';
    this.eGui.addEventListener('click', () => {
      const event = new CustomEvent('TELUS_CLICK', { detail: params.data });
      window.dispatchEvent(event);
    });
  }
  getGui(){ return this.eGui; }
}
""")

# -------------------------------------------------------------------
# Build grid options
# -------------------------------------------------------------------
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_column("Aircraft", cellRenderer=link_renderer)
gb.configure_column("Δ (min)", width=90, type=["numericColumn"])
gb.configure_column("Status", cellStyle=status_style)
gb.configure_column("Action", cellRenderer=button_renderer, 
                    maxWidth=160, editable=False, sortable=False)
gb.configure_default_column(resizable=True, wrapText=True, autoHeight=True)
grid_options = gb.build()

st.markdown("### ✈️ Flight-Following Dashboard — AgGrid Interactive View")

grid_response = AgGrid(
    df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    fit_columns_on_grid_load=True,
    allow_unsafe_jscode=True,   # enable JS renderers
    height=400,
    theme="streamlit",
)

# -------------------------------------------------------------------
# Handle JS button click events (TELUS posting)
# -------------------------------------------------------------------
event = st.session_state.get("telus_event")
# In production you'd attach a Streamlit-JS bridge or poll via grid_response, 
# but for simplicity here just illustrate the idea:
st.info("Click a 📣 button in the grid — the JS event can trigger your webhook.")

# Example placeholder webhook logic
# if event:
#     flight = event['data']
#     notify_delay_chat(team="FF", tail=flight['Aircraft'], booking=flight['Booking'],
#                       minutes_delta=flight['Δ (min)'], new_eta_hhmm=flight['ETA (UTC)'])
#     st.success(f"Posted {flight['Aircraft']} to TELUS")
