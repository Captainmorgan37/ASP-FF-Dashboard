import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

st.set_page_config(layout="wide")

# -------------------------------------------------------------------
# Mock data – replace with your real FL3XX / FlightAware dataframe
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

# ✅ FlightAware clickable link renderer
link_renderer = JsCode("""
function(params) {
  if (!params.value) return '';
  const url = 'https://www.flightaware.com/live/flight/' + params.value.replace('-', '');
  return '<span><a href="' + url + '" target="_blank" style="color:#4da6ff;text-decoration:none;">' 
         + params.value + '</a></span>';
}
""")

# ✅ Conditional coloring for Status column
status_style = JsCode("""
function(params){
  if (['Delayed','Late','ATC Delay'].includes(params.value))
      return {'color':'#fff','backgroundColor':'#b33a3a'};
  if (params.value === 'On Time')
      return {'color':'#fff','backgroundColor':'#2d8031'};
  return {'color':'#ddd','backgroundColor':'#444'};
}
""")

# ✅ Button renderer (only appears for delayed-type rows)
button_renderer = JsCode("""
class BtnCellRenderer {
  init(params){
    this.params = params;
    this.eGui = document.createElement('div');
    const st = params.data.Status;
    if (['Delayed','Late','ATC Delay'].includes(st)) {
        const b = document.createElement('button');
        b.innerText = '📣 Post to TELUS';
        b.style.background = '#444';
        b.style.color = 'white';
        b.style.border = 'none';
        b.style.borderRadius = '4px';
        b.style.cursor = 'pointer';
        b.style.padding = '2px 8px';
        b.addEventListener('click', () => {
          const event = new CustomEvent('TELUS_CLICK', { detail: params.data });
          window.dispatchEvent(event);
        });
        this.eGui.appendChild(b);
    } else {
        this.eGui.innerHTML = ''; // no button for on-time flights
    }
  }
  getGui(){ return this.eGui; }
}
""")

# -------------------------------------------------------------------
# Build grid options
# -------------------------------------------------------------------
gb = GridOptionsBuilder.from_dataframe(df)

gb.configure_column(
    "Aircraft",
    headerName="Aircraft",
    cellRenderer=link_renderer,
    autoHeight=True,
    wrapText=False,
)
gb.configure_column("Δ (min)", width=90, type=["numericColumn"])
gb.configure_column("Status", cellStyle=status_style)
gb.configure_column("Action", cellRenderer=button_renderer,
                    maxWidth=160, editable=False, sortable=False)
gb.configure_default_column(resizable=True, wrapText=True, autoHeight=True)
grid_options = gb.build()

# -------------------------------------------------------------------
# Render grid
# -------------------------------------------------------------------
st.markdown("### ✈️ Flight-Following Dashboard — AgGrid Interactive View")

AgGrid(
    df,
    gridOptions=grid_options,
    update_mode=GridUpdateMode.NO_UPDATE,
    fit_columns_on_grid_load=True,
    allow_unsafe_jscode=True,   # 🔥 absolutely required for JS renderers
    enable_enterprise_modules=False,
    height=400,
    theme="balham",
)
