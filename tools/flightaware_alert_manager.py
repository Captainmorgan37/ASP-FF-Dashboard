#!/usr/bin/env python3
"""Command line helper for configuring FlightAware AeroAPI flight alerts.

The tool can operate against the live AeroAPI alerts endpoint or in a
self-contained sandbox mode that stores alert definitions on disk. This makes
it easy to experiment with alert configuration without touching production
settings and provides a repeatable way to seed alerts for the dashboard.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional, Sequence

from flightaware_alerts import (
    DEFAULT_FLIGHT_ALERT_EVENTS,
    FlightAwareAlert,
    FlightAwareApiConfig,
    configure_test_alerts,
    delete_alert_subscription,
    ensure_alert_subscription,
    list_alerts,
    set_default_alert_endpoint,
)


def _normalise_events(events: Optional[Sequence[str]]) -> List[str]:
    if not events:
        return list(DEFAULT_FLIGHT_ALERT_EVENTS)
    cleaned: List[str] = []
    seen = set()
    for event in events:
        lower = str(event).strip().lower()
        if not lower or lower in seen:
            continue
        seen.add(lower)
        cleaned.append(lower)
    return cleaned or list(DEFAULT_FLIGHT_ALERT_EVENTS)


def _load_tails(args: argparse.Namespace) -> List[str]:
    tails: List[str] = []
    if getattr(args, "tails", None):
        tails.extend(args.tails)
    tails_file = getattr(args, "tails_file", None)
    if tails_file:
        path = Path(tails_file)
        if not path.exists():
            raise SystemExit(f"tails file not found: {path}")
        with path.open("r", encoding="utf8") as handle:
            for line in handle:
                tail = line.strip()
                if tail:
                    tails.append(tail)
    unique = []
    seen = set()
    for tail in tails:
        normalized = tail.strip()
        if not normalized:
            continue
        if normalized.upper() in seen:
            continue
        seen.add(normalized.upper())
        unique.append(normalized)
    if not unique:
        raise SystemExit("no tail numbers supplied")
    return unique


def _format_alert(alert: FlightAwareAlert) -> str:
    parts = [f"id={alert.alert_id or '-'}", f"tail={alert.identifier}"]
    if alert.description:
        parts.append(f"desc={alert.description}")
    parts.append("events=" + ",".join(alert.events))
    if alert.target_url:
        parts.append(f"target={alert.target_url}")
    return " | ".join(parts)


def _print_alerts(alerts: Sequence[FlightAwareAlert]) -> None:
    if not alerts:
        print("No alerts configured.")
        return
    for alert in alerts:
        print("- " + _format_alert(alert))


class SandboxStore:
    """Persist alert definitions to a JSON file for offline experiments."""

    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if not self.path.exists():
            return {"alerts": [], "endpoint": None, "_next_id": 1}
        with self.path.open("r", encoding="utf8") as handle:
            try:
                payload = json.load(handle)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"failed to read sandbox file: {exc}")
        payload.setdefault("alerts", [])
        payload.setdefault("endpoint", None)
        payload.setdefault("_next_id", 1)
        return payload

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf8") as handle:
            json.dump(self.data, handle, indent=2, sort_keys=True)

    @staticmethod
    def _matches_tail(existing_tail: str, candidate: str) -> bool:
        return existing_tail.replace("-", "").upper() == candidate.replace("-", "").upper()

    def list_alerts(self) -> List[FlightAwareAlert]:
        return [FlightAwareAlert.from_payload(item) for item in self.data.get("alerts", [])]

    def ensure_alert(
        self,
        tail: str,
        events: Sequence[str],
        description: Optional[str],
        target_url: Optional[str],
    ) -> FlightAwareAlert:
        alerts = self.data.setdefault("alerts", [])
        for item in alerts:
            if self._matches_tail(str(item.get("ident") or item.get("identifier", "")), tail):
                item["ident"] = tail
                item["events"] = list(events)
                if description is not None:
                    item["description"] = description
                if target_url is not None:
                    item["target_url"] = target_url
                self._save()
                return FlightAwareAlert.from_payload(item)
        next_id = str(self.data.setdefault("_next_id", 1))
        self.data["_next_id"] = int(next_id) + 1
        payload = {
            "id": next_id,
            "ident": tail,
            "events": list(events),
        }
        if description is not None:
            payload["description"] = description
        if target_url is not None:
            payload["target_url"] = target_url
        alerts.append(payload)
        self._save()
        return FlightAwareAlert.from_payload(payload)

    def delete_alert(self, alert_id: str) -> bool:
        alerts = self.data.setdefault("alerts", [])
        before = len(alerts)
        alerts[:] = [item for item in alerts if str(item.get("id")) != str(alert_id)]
        changed = len(alerts) != before
        if changed:
            self._save()
        return changed

    def set_endpoint(self, target_url: str) -> None:
        self.data["endpoint"] = target_url
        self._save()

    def get_endpoint(self) -> Optional[str]:
        return self.data.get("endpoint")


def build_config(args: argparse.Namespace) -> FlightAwareApiConfig:
    api_key = args.api_key or os.environ.get("FLIGHTAWARE_API_KEY")
    if not args.sandbox and not api_key:
        raise SystemExit("an AeroAPI key is required unless --sandbox is used")
    headers = {}
    for raw_header in args.extra_header or []:
        if ":" not in raw_header:
            raise SystemExit(f"invalid header format: {raw_header}")
        key, value = raw_header.split(":", 1)
        headers[key.strip()] = value.strip()
    return FlightAwareApiConfig(
        base_url=args.base_url,
        api_key=api_key,
        extra_headers=headers or None,
        verify_ssl=not args.insecure,
        timeout=args.timeout,
    )


def handle_list(args: argparse.Namespace, config: FlightAwareApiConfig) -> None:
    if args.sandbox:
        store = SandboxStore(Path(args.sandbox))
        _print_alerts(store.list_alerts())
        endpoint = store.get_endpoint()
        if endpoint:
            print(f"Default endpoint: {endpoint}")
        return

    alerts = list_alerts(config)
    _print_alerts(alerts)


def handle_ensure(args: argparse.Namespace, config: FlightAwareApiConfig) -> None:
    tails = _load_tails(args)
    events = _normalise_events(args.events)
    description_prefix = args.description_prefix
    target_url = getattr(args, "target_url", None)

    if args.sandbox:
        store = SandboxStore(Path(args.sandbox))
        alerts = []
        for tail in tails:
            description = f"{description_prefix} {tail}".strip()
            alerts.append(store.ensure_alert(tail, events, description, target_url))
        print("Sandbox alerts updated:")
        _print_alerts(alerts)
        endpoint = store.get_endpoint()
        if endpoint:
            print(f"Default endpoint: {endpoint}")
        return

    if len(tails) == 1:
        alert = ensure_alert_subscription(
            config,
            tails[0],
            events=events,
            description=f"{description_prefix} {tails[0]}".strip(),
            target_url=target_url,
        )
        print("Alert ensured:")
        _print_alerts([alert])
    else:
        alerts = configure_test_alerts(
            config,
            tails,
            events=events,
            description_prefix=description_prefix,
            target_url=target_url,
        )
        print("Alerts ensured:")
        _print_alerts(alerts)


def handle_set_endpoint(args: argparse.Namespace, config: FlightAwareApiConfig) -> None:
    if args.sandbox:
        store = SandboxStore(Path(args.sandbox))
        store.set_endpoint(args.target_url)
        print(f"Sandbox endpoint set to {args.target_url}")
        return

    response = set_default_alert_endpoint(config, args.target_url)
    print("Default endpoint updated:")
    print(json.dumps(response, indent=2))


def handle_delete(args: argparse.Namespace, config: FlightAwareApiConfig) -> None:
    if args.sandbox:
        store = SandboxStore(Path(args.sandbox))
        removed = []
        for alert_id in args.alert_ids:
            if store.delete_alert(alert_id):
                removed.append(alert_id)
        if removed:
            print("Removed sandbox alerts:", ", ".join(removed))
        else:
            print("No matching sandbox alerts were removed.")
        return

    for alert_id in args.alert_ids:
        delete_alert_subscription(config, alert_id)
        print(f"Deleted alert {alert_id}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-key", help="FlightAware AeroAPI key (or set FLIGHTAWARE_API_KEY)")
    parser.add_argument(
        "--base-url",
        default="https://aeroapi.flightaware.com/aeroapi/alerts",
        help="Base alerts endpoint (default: %(default)s)",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification")
    parser.add_argument(
        "--extra-header",
        action="append",
        help="Additional HTTP header in the form 'Header: Value' (can be repeated)",
    )
    parser.add_argument(
        "--sandbox",
        metavar="PATH",
        help="Operate against a local JSON sandbox instead of the live API",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List configured alerts")
    list_parser.set_defaults(func=handle_list)

    ensure_parser = subparsers.add_parser("ensure", help="Ensure alerts exist for the supplied tails")
    ensure_parser.add_argument("tails", nargs="*", help="Tail numbers to configure")
    ensure_parser.add_argument("--tails-file", help="Path to a file containing additional tails (one per line)")
    ensure_parser.add_argument(
        "--events",
        nargs="+",
        help="Override the alert events (default: out off on in)",
    )
    ensure_parser.add_argument(
        "--description-prefix",
        default="Test Flight Alert",
        help="Prefix to use for alert descriptions",
    )
    ensure_parser.add_argument(
        "--target-url",
        help="Override the delivery URL for the alert (defaults to account endpoint)",
    )
    ensure_parser.set_defaults(func=handle_ensure)

    endpoint_parser = subparsers.add_parser("set-endpoint", help="Set the account-wide alert delivery endpoint")
    endpoint_parser.add_argument("target_url", help="URL that FlightAware should POST alert payloads to")
    endpoint_parser.set_defaults(func=handle_set_endpoint)

    delete_parser = subparsers.add_parser("delete", help="Delete alerts by id")
    delete_parser.add_argument("alert_ids", nargs="+", help="Identifiers returned by the list command")
    delete_parser.set_defaults(func=handle_delete)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = build_config(args)
    args.func(args, config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
