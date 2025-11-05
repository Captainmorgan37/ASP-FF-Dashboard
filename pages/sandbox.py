import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

st.set_page_config(layout="wide")

# -------------------------------------------------------------------
# Mock flight data (replace with live FL3XX / FlightAware merge)
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
# JavaScript renderers
# -------------------------------------------------------------------

# Aircraft → FlightAware hyperlink
link_renderer = JsCode("""
function(params) {
  if (!params.value) return '';
  const url = 'https://www.flightaware.com/live/flight/' + params.value.replace('-', '');
  return `<a href="${url}" target="_blank"
             style="color:#4da6ff;text-decoration:none;">${params.value}</a>`;
}
""")

# Status coloring
status_style = JsCode("""
function(params){
  if (['Delayed','Late','ATC Delay'].includes(params.value))
      return {'color':'#fff','backgroundColor':'#b33a3a'};
  if (params.value === 'On Time')
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
    Object.assign(this.eGui.style,{
      background:'#444',color:'white',border:'none',borderRadius:'4px',
      cursor:'pointer',padding:'2px 8px'
    });
    this.eGui.addEventListener('click', () => {
      const event = new CustomEvent('TELUS_CLICK', { detail: params.data });
      window.dispatchEvent(event);
    });
  }
  getGui(){ return this.eGui; }
}
""")

# -------------------------------------------------------------------
# Grid configuration
# -------------------------------------------------------------------
gb = GridOptionsBuilder.from_dataframe(df)

# Here’s the key part: we must explicitly set 'cellRenderer' and 'cellRendererParams'!
gb.configure_column(
    "Aircraft",
    headerName="Aircraft",
    cellRenderer=link_renderer,
    autoHeight=True,
    wrapText=False,
    tooltipField="Aircraft",
)

gb.configure_column("Δ (min)", width=90, type=["numericColumn"])
gb.configure_column("Status", cellStyle=status_style)
gb.configure_column("Action", cellRenderer=button_renderer,
                    maxWidth=160, editable=False, sortable=False)

# ensure all columns allow JS rendering
gb.configure_default_column(resizable=True, wrapText=True, autoHeight=True)

grid_options = gb.build()

# -------------------------------------------------------------------
# Grid render
# -------------------------------------------------------------------
st.markdown("### ✈️ Flight-Following Dashboard — AgGrid Interactive View")

AgGrid(
    df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    allow_unsafe_jscode=True,   # <- required for JS renderers
    enable_enterprise_modules=False,
    fit_columns_on_grid_load=True,
    height=400,
    theme="streamlit",
)
