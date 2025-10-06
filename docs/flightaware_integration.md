# FlightAware Integration Overview

This project currently exercises the FlightAware AeroAPI integration through the
`flightaware_alerts` module and its accompanying unit tests. The Streamlit UI in
`ASP FF Dashboard.py` still relies on e-mail alerts delivered to the IMAP inbox,
so updating the tests alone will not surface a visible change in the dashboard.

## Where the FlightAware API Is Used

* `flightaware_alerts.py` centralises all outbound calls to the AeroAPI Alerts
  endpoint. The module accepts a `FlightAwareApiConfig` that determines the base
  URL, headers, and timeout behaviour for every request, and provides helpers
  for listing, creating, and updating alert subscriptions.【F:flightaware_alerts.py†L10-L145】
* `tests/test_flightaware_alerts.py` verifies the behaviour of the alert helper
  functions by faking HTTP sessions. Updating the tests exercises the Python
  client logic only; no Streamlit components are touched.【F:tests/test_flightaware_alerts.py†L1-L103】

## Updating FlightAware Behaviour

* To change how the dashboard communicates with AeroAPI (for example, to add new
  headers or adjust the subscription payload), modify the functions in
  `flightaware_alerts.py` and extend the related tests to cover the new
  behaviour.【F:flightaware_alerts.py†L94-L199】【F:tests/test_flightaware_alerts.py†L44-L103】
* The Streamlit UI currently consumes FlightAware data indirectly from e-mail by
  connecting to the configured IMAP mailbox (`IMAP_SENDER` and related secrets).
  Because the UI does not yet call the AeroAPI helpers, changing those helpers
  will not immediately alter what appears on screen. To wire the API into the
  dashboard you would import `flightaware_alerts` inside `ASP FF Dashboard.py`
  and replace or augment the existing IMAP processing logic.【F:ASP FF Dashboard.py†L1-L210】【F:ASP FF Dashboard.py†L3006-L3099】

## Where to Update API Calls

* In-app changes (Streamlit): Integrate the helpers directly in `ASP FF
  Dashboard.py` if you want API-driven data to appear in the UI. This will
  require wiring in configuration (API key, base URL) via `st.secrets` or
  another secure mechanism, and replacing parts of the schedule/alert ingestion
  pipeline with API-backed data.
* External automation or scripts: If alert management should run outside the
  dashboard (for example, as a CLI or scheduled job), you can invoke the
  functions in `flightaware_alerts.py` from a separate script without touching
  the Streamlit application. The module is designed to be reusable thanks to the
  pure-Python request session abstractions.【F:flightaware_alerts.py†L94-L199】

In short, the updated tests confirm that the Python client is ready for use, but
additional UI plumbing is needed before any changes appear in the live dashboard.
