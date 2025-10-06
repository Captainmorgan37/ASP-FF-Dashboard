# FlightAware Integration Overview

This project currently exercises the FlightAware AeroAPI integration through the
`flightaware_alerts` module and its accompanying unit tests. The Streamlit UI in
`ASP FF Dashboard.py` still relies on e-mail alerts delivered to the IMAP inbox,
so updating the tests alone will not surface a visible change in the dashboard.

## Where the FlightAware API Is Used

* `flightaware_alerts.py` centralises all outbound calls to the AeroAPI Alerts
  endpoint. The module accepts a `FlightAwareApiConfig` that determines the base
  URL, headers, and timeout behaviour for every request, and provides helpers
  for listing, creating, and updating alert subscriptions.【F:flightaware_alerts.py†L10-L199】
* `tests/test_flightaware_alerts.py` verifies the behaviour of the alert helper
  functions by faking HTTP sessions. Updating the tests exercises the Python
  client logic only; no Streamlit components are touched.【F:tests/test_flightaware_alerts.py†L1-L103】

### Exact HTTP traffic

* When the Streamlit toggle **Use FlightAware AeroAPI for status updates** is
  enabled **and** credentials are provided, the dashboard performs a `GET`
  request to `/flights/{ident}` for each aircraft tail on the current schedule
  in order to hydrate departure/arrival events.【F:ASP FF Dashboard.py†L2132-L2195】【F:flightaware_status.py†L39-L92】
* The application does **not** invoke any alert endpoints today. The only code
  that exercises `/alerts`, `PUT /alerts/{id}`, or related routes lives in the
  reusable helpers and their unit tests; they are not executed by the
  Streamlit runtime.【F:ASP FF Dashboard.py†L2132-L2195】【F:flightaware_alerts.py†L85-L199】
* As shipped, there are therefore no AeroAPI calls at all unless the dashboard
  operator opts into the status fetch toggle or imports the helpers in an
  external automation.

To meet a “alerts only” requirement you can leave the toggle disabled (this is
the default unless the operator explicitly turns it on) and rely on FlightAware
alert deliveries via IMAP, or wire the alert helpers into your own provisioning
script. The repository now ships with a dedicated CLI wrapper,
`tools/flightaware_alert_manager.py`, to make that setup straightforward. It
exposes `list`, `ensure`, `set-endpoint`, and `delete` sub-commands that call the
existing helper functions and can operate against either the live AeroAPI
service or a JSON-backed sandbox for experimentation.【F:tools/flightaware_alert_manager.py†L1-L230】

* `python tools/flightaware_alert_manager.py --sandbox sandbox.json ensure N556FF`
  seeds alerts in a local file without touching the network. The sandbox stores
  the default endpoint and alert payloads so you can iterate safely before
  copying the configuration into production.【F:tools/flightaware_alert_manager.py†L59-L164】
* `python tools/flightaware_alert_manager.py --api-key <KEY> set-endpoint https://example/app`
  performs the required `PUT /alerts/endpoint` call. Subsequent `ensure`
  commands create or update tail-specific alerts (including optional
  `--target-url` overrides) using the same API key.【F:tools/flightaware_alert_manager.py†L166-L215】

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
