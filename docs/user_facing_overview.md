# Flight Following Dashboard – User Overview

This document explains how the Flight Following Dashboard behaves from the perspective of flight followers and dispatch staff. Use it when defining SOPs or onboarding team members who will run the enhanced flight following workflow.

## Core purpose

The dashboard centralizes the near-term FL3XX schedule so teams can monitor flights by phase, spotlight flights needing enhanced flight following, and keep a log of outbound notifications. The interface runs in a browser via NiceGUI and can fall back to Streamlit if launched with `streamlit run app.py`.【F:app.py†L10-L17】【F:app.py†L256-L338】

## Loading a schedule

Users start by populating the working schedule:

1. **Upload a CSV** exported from FL3XX or a similar system. Use the uploader in the "Schedule status" card. The dashboard parses the file, records the filename and upload timestamp, and refreshes every table automatically.【F:app.py†L307-L346】
2. **Fetch via FL3XX API (demo).** The "Load sample flight" button at the top header simulates what a real FL3XX API pull would do by normalizing the API payload into the standard table layout.【F:app.py†L507-L538】

Once data is loaded, the status label at the top of the dashboard shows the source, filename, timestamp, and how many flights are being tracked.【F:app.py†L228-L252】【F:app.py†L287-L300】

## Table layout and flight phases

The main schedule card is split into three expanding tables: **Landed**, **Enroute**, and **To Depart**. Each phase still shows the same core identifiers (booking ID, routing, crew, account, aircraft, type, workflow), but columns that are irrelevant to that phase are automatically hidden. For example, departures no longer list ETA or arrival countdown columns, while landed flights suppress the "Departs In" countdown so only useful context remains.【F:app.py†L140-L244】【F:app.py†L624-L676】

Flights move between phases automatically based on both keyword cues (e.g., "Blocks Off" or "Arrived" values) and timestamp fields such as actual off/landing times. If neither landed nor airborne cues are present, the flight stays in "To Depart."【F:app.py†L207-L270】

This structure lets dispatchers assign staff to specific operational windows: one person can watch landed flights for post-arrival tasks, another can track airborne flights, and another can concentrate on departures.

## Enhanced Flight Following workflow

The second card on the page adds tooling for escalated monitoring:

1. Toggle **Enhanced Flight Following Requested** when the duty pilot or operations manager needs closer oversight.【F:app.py†L566-L608】
2. Select one or more bookings from the multi-select list. Entries combine the booking reference with its route so users can quickly confirm they picked the correct leg.【F:app.py†L404-L461】
3. The table underneath immediately filters to the selected bookings and reflects any schedule refresh. If a monitored flight disappears from the schedule, the message label warns that the selection no longer exists so staff can investigate.【F:app.py†L461-L505】

This design keeps the core schedule intact while offering a laser-focused panel for high-priority flights that need callouts every 15 minutes or other bespoke tracking.

## Inline notifications and log

Inside the status card, team members can type a note and hit **Send inline notification**. The action timestamps the message, shows a success toast, and archives the entry in the "Notification history" expansion log so the entire team can audit outbound calls or emails.【F:app.py†L346-L382】【F:app.py†L604-L618】

## Secrets diagnostics panel

The final card summarizes which integrations (FlightAware, FL3XX, etc.) have credentials configured. The **Refresh** button reruns the diagnostics collector and updates a tabular view that lists each secret, its status, and any warnings. Dispatchers can use this panel before a shift change to confirm all back-end connections are healthy without digging into configuration files.【F:app.py†L300-L345】【F:app.py†L666-L701】

## Bringing it together for SOP design

When drafting the staff workflow:

- Start each shift by loading the latest schedule and confirming the status panel shows the correct source and timestamp.
- Assign individuals to monitor each phase table; use the expansion toggles to collapse irrelevant phases when staffing is tight.
- Enable Enhanced Flight Following only for legs that need additional scrutiny, and document the monitoring cadence in the notification log for traceability.
- Use the secrets diagnostics card as part of the shift checklist so teams catch integration issues before they impact flight following.

By mirroring these UI elements in your SOP, staff can confidently follow a repeatable process that aligns with the dashboard's capabilities.
