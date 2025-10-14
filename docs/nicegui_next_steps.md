# Bringing real data into the NiceGUI dashboard

The App Runner deployment renders the NiceGUI shell from `app.py`, but it only
shows placeholder content until the FL3XX schedule loader and the FlightAware
webhook reader are wired in. Follow the steps below to make the hosted version
match the Streamlit feature set.

## 1. Confirm the demo scaffold is running

* Uploading a CSV still routes through `data_sources.load_schedule()`, but the
  handler now traps runtime errors and surfaces them as toast notifications so
  you can diagnose malformed files from App Runner logs.【F:app.py†L156-L205】
* The "Load sample flight" button now feeds a canned FL3XX flight through the
  same normalization helper that the production API call will use. Missing
  dependencies (for example `data_sources` or `pandas`) produce a warning
  instead of silently doing nothing.【F:app.py†L207-L263】

Keeping the demo behaviour intact is helpful while you layer in external
integrations.

## 2. Provide the same secrets that Streamlit expects

The Streamlit UI already knows how to talk to FL3XX and DynamoDB once the
relevant secrets are present. Reuse the configuration model to keep both
front-ends aligned.

### FL3XX API credentials

* Define an App Runner secret named `FL3XX_API_TOKEN` or add a structured
  `[fl3xx_api]` block (with `api_token` or `auth_header`) via the console. The
  Streamlit helpers look for the same keys inside `_build_fl3xx_config_from_secrets()`【F:ASP FF Dashboard.py†L1027-L1100】
  and refuse to call the API when no auth material is provided.【F:ASP FF Dashboard.py†L1267-L1289】
* If the production environment uses a custom header name, also set
  `FL3XX_AUTH_HEADER_NAME`—the existing `Fl3xxApiConfig` object honours that at
  request time.【F:fl3xx_client.py†L14-L184】

### FlightAware webhook / DynamoDB

* Supply `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` as App
  Runner secrets. The diagnostics table in the Streamlit build checks those keys
  before it attempts to reach DynamoDB.【F:ASP FF Dashboard.py†L580-L625】
* Point the NiceGUI app at the same table name. If you do not set
  `FLIGHTAWARE_ALERTS_TABLE`, the Streamlit module falls back to `fa-oooi-alerts`,
  so mirror that default unless your pipeline uses a different table.【F:ASP FF Dashboard.py†L597-L605】

Once the secrets exist in App Runner you can read them from `os.environ` inside
`app.py` or share a helper module that mirrors the Streamlit logic.

## 3. Reuse the shared data loaders

Two modules already expose the transformations you need:

* `data_sources.load_schedule()` converts raw FL3XX flights into the CSV-friendly
  dataframe the table expects.【F:data_sources.py†L140-L186】
* `fl3xx_client.fetch_flights()` (plus `enrich_flights_with_crew`) handles the
  API calls, caching, and digest tracking that the Streamlit version uses before
  it hands the flights to the scheduler.【F:ASP FF Dashboard.py†L1278-L1320】

Update `simulate_fetch_from_fl3xx()` (or add a new handler) so that it builds a
real `Fl3xxApiConfig` from secrets, calls the client functions, and feeds the
returned flights through `load_schedule("fl3xx_api", metadata={...})`. That will
populate the NiceGUI table with live data just like the Streamlit tab does.

## 4. Surface FlightAware webhook status

The Streamlit app polls DynamoDB when the "Use FlightAware webhook alerts" toggle
is enabled.【F:ASP FF Dashboard.py†L3207-L3345】 Replicate that flow by:

1. Copying the configuration reader (`build_flightaware_webhook_config()` and
   the associated diagnostics) into a shared module so `app.py` can construct a
   boto3 client with the same table name and region.
2. Invoking `fetch_flightaware_webhook_events()` with the active schedule's
   idents and writing the returned records into a NiceGUI log/table.
3. Optionally reusing `apply_flightaware_webhook_updates()` so the OOOI states
   update the schedule rows in place before rendering.

Because the Streamlit code already cleanly separates configuration, fetching, and
rendering, porting those helpers will give the NiceGUI frontend feature parity
without re-implementing the business logic.

## 5. Test locally before redeploying

Run the NiceGUI app on your workstation with the same environment variables you
plan to inject into App Runner. Once the table shows live flights and the
webhook diagnostics return records, push the changes so App Runner picks up the
new code automatically.

