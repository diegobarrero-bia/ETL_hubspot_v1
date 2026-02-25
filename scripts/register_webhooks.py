"""
One-time script to create/update HubSpot webhook subscriptions.
Run manually during deployment or via CI/CD.

Usage:
    python scripts/register_webhooks.py --config webhook_subscriptions.yaml
    python scripts/register_webhooks.py --config webhook_subscriptions.yaml --list
    python scripts/register_webhooks.py --config webhook_subscriptions.yaml --dry-run

Requires:
    - HUBSPOT_ACCESS_TOKEN env var (or --token flag)
    - A valid webhook_subscriptions.yaml config file
"""

import argparse
import json
import os
import sys

import requests
import yaml

HUBSPOT_API = "https://api.hubapi.com"


def load_config(config_path: str) -> dict:
    """Load and validate the webhook subscriptions YAML config."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    if not config or "subscriptions" not in config:
        print("Error: Config file must contain a 'subscriptions' key.")
        sys.exit(1)

    if not config.get("app_id") or config["app_id"] == "YOUR_HUBSPOT_APP_ID":
        print("Error: Set a valid 'app_id' in the config file.")
        sys.exit(1)

    return config


def list_subscriptions(app_id: str, headers: dict) -> list:
    """List existing webhook subscriptions for the app."""
    url = f"{HUBSPOT_API}/webhooks/v3/{app_id}/subscriptions"
    resp = requests.get(url, headers=headers, timeout=30)

    if resp.status_code != 200:
        print(f"Error listing subscriptions: {resp.status_code} — {resp.text}")
        return []

    results = resp.json().get("results", [])
    return results


def register_subscriptions(app_id: str, headers: dict, config: dict, dry_run: bool = False) -> dict:
    """
    Create webhook subscriptions from config.

    Returns:
        dict with counters: {"created": N, "exists": N, "failed": N}
    """
    summary = {"created": 0, "exists": 0, "failed": 0}

    for sub in config["subscriptions"]:
        object_type = sub["objectType"]
        for event_type in sub["eventTypes"]:
            payload = {
                "eventType": event_type,
                "propertyName": None,
                "active": True,
            }

            if dry_run:
                print(f"  [DRY RUN] Would create: {event_type} (object: {object_type})")
                summary["created"] += 1
                continue

            resp = requests.post(
                f"{HUBSPOT_API}/webhooks/v3/{app_id}/subscriptions",
                headers=headers,
                json=payload,
                timeout=30,
            )

            if resp.status_code == 201:
                print(f"  Created: {event_type}")
                summary["created"] += 1
            elif resp.status_code == 409:
                print(f"  Already exists: {event_type}")
                summary["exists"] += 1
            else:
                print(f"  Failed: {event_type} -> {resp.status_code}: {resp.text}")
                summary["failed"] += 1

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Register HubSpot webhook subscriptions from YAML config."
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to webhook_subscriptions.yaml",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("HUBSPOT_ACCESS_TOKEN"),
        help="HubSpot access token (defaults to HUBSPOT_ACCESS_TOKEN env var)",
    )
    parser.add_argument(
        "--list", action="store_true", dest="list_subs",
        help="List existing subscriptions and exit",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be created without making API calls",
    )

    args = parser.parse_args()

    if not args.token:
        print("Error: Provide --token or set HUBSPOT_ACCESS_TOKEN env var.")
        sys.exit(1)

    config = load_config(args.config)
    app_id = config["app_id"]
    headers = {
        "Authorization": f"Bearer {args.token}",
        "Content-Type": "application/json",
    }

    if args.list_subs:
        print(f"\nExisting subscriptions for app {app_id}:\n")
        subs = list_subscriptions(app_id, headers)
        if not subs:
            print("  (none)")
        for s in subs:
            status = "ACTIVE" if s.get("active") else "INACTIVE"
            print(f"  [{status}] {s.get('eventType', '?')} (id: {s.get('id', '?')})")
        print(f"\nTotal: {len(subs)}")
        return

    print(f"\nRegistering subscriptions for app {app_id}...")
    if args.dry_run:
        print("(DRY RUN — no API calls will be made)\n")
    else:
        print()

    summary = register_subscriptions(app_id, headers, config, dry_run=args.dry_run)

    print(f"\nDone: {summary['created']} created, {summary['exists']} already existed, {summary['failed']} failed")

    if summary["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
