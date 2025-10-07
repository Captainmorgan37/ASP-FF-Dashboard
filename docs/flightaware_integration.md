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

## AWS Webhook Ingestion Pipeline

### Prerequisites for the Streamlit webhook toggle

The Streamlit checkbox **“Use FlightAware webhook alerts (DynamoDB)”** only
appears when the app can import `boto3` and read the required AWS settings. If
you are running the dashboard locally and the toggle is missing, install the AWS
SDK for Python before launching Streamlit:

```bash
python -m pip install --upgrade boto3
```

The package is already listed in `requirements.txt`, so alternatively you can
install every dependency in one step:

```bash
python -m pip install --upgrade -r requirements.txt
```

After installing `boto3`, make sure the following secrets (or environment
variables) are populated so that `build_flightaware_webhook_config()` succeeds:

* `AWS_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

Launch the dashboard again and the webhook toggle will render when those values
are available.

#### Troubleshooting “Missing secrets”

If the diagnostics panel still reports `Missing secrets: AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY`, it means the app could not read **non-empty** values for one or more of those keys. The `_resolve_secret()` helper trims whitespace and treats empty strings as “missing,” so the entries must be present either in `.streamlit/secrets.toml` or as environment variables with actual values. After updating secrets, restart the Streamlit app so that `st.secrets` is reloaded. Once the credentials are visible to the app, the warning will disappear; any subsequent issues (for example, IAM permission errors) will surface as a different DynamoDB connection error instead of the “Missing secrets” message.

> **Security note:** Do not commit real credentials to the repository. A public GitHub project exposes every tracked file—including `.streamlit/secrets.toml`—to anyone who clones or forks it. Keep production secrets in a local, untracked `secrets.toml` (or inject them via environment variables in your deployment platform) and rely on placeholders or mocked values inside the repo.

The production FlightAware alerts are currently processed through a managed AWS
pipeline that feeds the Streamlit dashboard. The high-level flow is:

```
FlightAware AeroAPI (alerts) → API Gateway (HTTP API) → Lambda (Python) →
DynamoDB → Streamlit app (polls table every ~10 s)
```

All infrastructure lives in the `us-east-2` region.

### API Gateway (`fa-oooi-api`)

* **Type:** HTTP API with a `$default` stage.
* **Route:** `POST /fa/post`
* **Invoke URL:** `https://cgkogti9qd.execute-api.us-east-2.amazonaws.com/fa/post?token=<SHARED_TOKEN>`
* **Integration:** Lambda proxy integration to `fa-oooi-webhook`.
* The shared token embedded in the invoke URL must match the Lambda environment
  variable described below.

### Lambda Webhook (`fa-oooi-webhook`)

* **Runtime:** Python 3.11.
* **Purpose:** Validates the `token` query string parameter, normalises the
  incoming AeroAPI payload, and writes an item into DynamoDB.
* **Environment variables:**
  * `DDB_TABLE=fa-oooi-alerts`
  * `SHARED_TOKEN=<same token as in API Gateway>`
* **Permissions:** Inline IAM policy granting `dynamodb:PutItem` on the target
  table.
* **Logging:** Default CloudWatch Logs group for the function.

### DynamoDB Table (`fa-oooi-alerts`)

* **Primary key:**
  * Partition key `ident` (string)
  * Sort key `received_at` (string, ISO 8601 UTC timestamp)
* **Optional attributes:** `ttl_epoch` for time-to-live expiry.
* Items include the normalised fields plus the raw payload, for example:

  ```json
  {
    "ident": "ASP501",
    "received_at": "2025-10-06T18:42:10Z",
    "event": "on",
    "aircraft": "C-FASP",
    "origin": "CYYC",
    "destination": "CYYZ",
    "source_ts": "1730835600",
    "raw": { "…": "FlightAware payload" }
  }
  ```

### IAM Access for Streamlit

* Programmatic IAM user `streamlit-dynamodb-reader` holds read-only permissions
  (`dynamodb:Query`, `dynamodb:DescribeTable`) on the table.
* Credentials are stored in Streamlit secrets:
  * `AWS_REGION=us-east-2`
  * `AWS_ACCESS_KEY_ID=<read-only key>`
  * `AWS_SECRET_ACCESS_KEY=<read-only secret>`

### FlightAware Alert Configuration

* Five tail-specific alerts exist for `ASP501`, `ASP653`, `ASP548`, `ASP556`,
  and `ASP668`.
* Each alert enables the `out`, `off`, `on`, and `in` events (others disabled).
* `target_url` is the API Gateway invoke URL with the shared token query
  string. The optional account-wide endpoint (`PUT /aeroapi/alerts/endpoint`)
  is not in use.

### Lambda Contract

* **Request expectations:** JSON body with keys such as `event`, `ident` or
  `fa_flight_id`, optional `aircraft`/`registration`, `origin`, `destination`,
  and `timestamp`.
* **Authentication:** Requires `?token=<SHARED_TOKEN>`; otherwise responds with
  `401`.
* **Responses:** `200 {"status": "ok"}` on success, `400` for malformed JSON.
* **Storage:** Successful requests call `PutItem` with the normalised structure
  outlined above.

## Next Steps for an All-AWS Enterprise Edition

To evolve the proof-of-concept into an enterprise-ready deployment that stays
entirely within AWS, prioritise the following streams of work:

1. **Harden the ingestion tier.** Add request validation (payload schemas, size
   limits) at API Gateway, enable WAF for IP allow/deny rules, and introduce a
   dead-letter queue (DLQ) on the Lambda to capture failed writes for replay.
   Define throttling limits and custom authorisers if third parties will post to
   the endpoint.【F:docs/flightaware_integration.md†L126-L146】
2. **Automate infrastructure provisioning.** Capture the API Gateway, Lambda,
   DynamoDB table, IAM roles, and supporting resources in AWS CDK, Terraform, or
   CloudFormation so environments (dev/stage/prod) can be recreated reliably.
   Pair this with AWS CodePipeline/CodeBuild or GitHub Actions for CI/CD, and
   include unit/integration tests plus canary deployments for the webhook.
3. **Elevate data lifecycle management.** Enable DynamoDB TTL on `ttl_epoch`,
   stream expired/archived records into Kinesis Firehose or Lambda for cold
   storage in S3/Glacier, and back-fill analytics needs with Athena/QuickSight.
   Consider partitioning by operator or business unit if additional fleets join.
4. **Deploy a managed presentation layer on AWS.** Containerise the Streamlit
   app and host it on App Runner, ECS Fargate, or EKS with an Application Load
   Balancer. Store secrets (read-only credentials, API keys) in AWS Secrets
   Manager, and front the UI with CloudFront plus AWS SSO/Cognito for managed
   authentication instead of embedding IAM keys in configuration.【F:docs/flightaware_integration.md†L85-L124】
5. **Improve observability and alerting.** Standardise structured logging,
   enable AWS X-Ray tracing, and create CloudWatch metrics/alarms for end-to-end
   latency, DynamoDB throttles, and Lambda errors. Surface dashboards in
   CloudWatch or Grafana and configure SNS/Slack alerts for operational events.
6. **Institutionalise security operations.** Rotate shared tokens via Secrets
   Manager, enforce IAM least privilege with SCPs, adopt AWS Config and Security
   Hub checks, and add guardrails (service control policies) in a multi-account
   landing zone. Apply encryption-at-rest (KMS customer-managed keys) and
   encryption-in-transit (TLS) everywhere.
7. **Support downstream integrations.** Publish alert updates to EventBridge or
   SNS so other enterprise systems (maintenance, crew ops) can subscribe without
   polling DynamoDB. Optionally expose read APIs (AppSync/GraphQL or REST) with
   fine-grained IAM/authorisation for partner access.

Completing these tracks yields a fully AWS-hosted, auditable, and scalable
platform that can onboard new aircraft or business units with minimal manual
intervention while meeting enterprise security and reliability requirements.

### Streamlit Consumption

* The dashboard polls DynamoDB roughly every 10 seconds using the read-only
  IAM credentials and `boto3` helpers inside `ASP FF Dashboard.py` to refresh
  per-ident timelines.【F:ASP FF Dashboard.py†L360-L505】【F:ASP FF Dashboard.py†L2502-L2506】
* Recent items are fetched with `KeyConditionExpression=Key("ident").eq(ident)`
  and `ScanIndexForward=False` so the latest events appear first.

### Testing and Operations

* Manual tests: `Invoke-RestMethod -Method Post -Uri <WEBHOOK_URL> -Body '{"event":"on","ident":"ASP501"}'`
  and confirm the record in DynamoDB.
* Monitoring: Review the Lambda’s CloudWatch Logs for delivery diagnostics.
* Security: Rotate the shared token by updating both the Lambda environment
  variable and the per-alert `target_url` values. The Streamlit user remains
  read-only to limit blast radius.
