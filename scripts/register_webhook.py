#!/usr/bin/env python3
"""
Script to register HubSpot webhooks for testing.

Usage:
    python scripts/register_webhook.py --app-id YOUR_APP_ID --object-type contact
    python scripts/register_webhook.py --list  # List all subscriptions
    python scripts/register_webhook.py --delete-all  # Clean up all subscriptions
"""
import argparse
import json
import os
import sys
from typing import Optional

import requests
from dotenv import load_dotenv


class HubSpotWebhookManager:
    """Manages HubSpot webhook subscriptions via API."""

    def __init__(self, app_id: int, access_token: str, webhook_url: str):
        self.app_id = app_id
        self.access_token = access_token
        self.webhook_url = webhook_url
        self.base_url = f"https://api.hubspot.com/webhooks/v3/{app_id}"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    def list_subscriptions(self) -> list[dict]:
        """List all active webhook subscriptions."""
        url = f"{self.base_url}/subscriptions"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json().get("results", [])

    def register_subscription(
        self,
        event_type: str,
        property_name: Optional[str] = None,
        active: bool = True,
    ) -> dict:
        """
        Register a new webhook subscription.

        Args:
            event_type: e.g., "contact.creation", "contact.propertyChange"
            property_name: Required for propertyChange events (e.g., "email")
            active: Whether the subscription is active
        """
        url = f"{self.base_url}/subscriptions"
        payload = {
            "active": active,
            "eventType": event_type,
        }
        if property_name:
            payload["propertyName"] = property_name

        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

    def delete_subscription(self, subscription_id: int) -> None:
        """Delete a webhook subscription."""
        url = f"{self.base_url}/subscriptions/{subscription_id}"
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()

    def delete_all_subscriptions(self) -> int:
        """Delete all webhook subscriptions. Returns count deleted."""
        subscriptions = self.list_subscriptions()
        for sub in subscriptions:
            self.delete_subscription(sub["id"])
        return len(subscriptions)

    def register_standard_events(self, object_type: str) -> list[dict]:
        """
        Register standard webhook events for an object type.

        Events registered:
        - {object_type}.creation
        - {object_type}.propertyChange (generic, no specific property)
        - {object_type}.deletion
        - {object_type}.associationChange

        Args:
            object_type: e.g., "contact", "deal", "company", "ticket"

        Returns:
            List of created subscription details
        """
        event_types = [
            f"{object_type}.creation",
            f"{object_type}.propertyChange",
            f"{object_type}.deletion",
            f"{object_type}.associationChange",
        ]

        results = []
        for event_type in event_types:
            try:
                result = self.register_subscription(event_type)
                results.append(result)
                print(f"✓ Registered: {event_type} (id: {result['id']})")
            except requests.HTTPError as e:
                print(f"✗ Failed to register {event_type}: {e}")
                if e.response.status_code == 409:
                    print(f"  (Subscription may already exist)")
                else:
                    print(f"  Response: {e.response.text}")

        return results


def main():
    parser = argparse.ArgumentParser(
        description="Manage HubSpot webhook subscriptions"
    )
    parser.add_argument(
        "--app-id",
        type=int,
        help="HubSpot app ID (or set HUBSPOT_APP_ID env var)",
    )
    parser.add_argument(
        "--access-token",
        help="HubSpot access token (or set HUBSPOT_ACCESS_TOKEN env var)",
    )
    parser.add_argument(
        "--webhook-url",
        help="Webhook URL (or set WEBHOOK_URL env var)",
    )
    parser.add_argument(
        "--object-type",
        choices=["contact", "deal", "company", "ticket", "line_item"],
        help="Object type to register webhooks for",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all current subscriptions",
    )
    parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete all webhook subscriptions",
    )
    parser.add_argument(
        "--env-file",
        default=".env.webhook",
        help="Path to .env file (default: .env.webhook)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv(args.env_file)

    # Get configuration
    app_id = args.app_id or os.getenv("HUBSPOT_APP_ID")
    access_token = args.access_token or os.getenv("HUBSPOT_ACCESS_TOKEN")
    webhook_url = args.webhook_url or os.getenv("WEBHOOK_URL")

    if not all([app_id, access_token]):
        print("Error: Missing required configuration", file=sys.stderr)
        print("Provide --app-id and --access-token, or set environment variables:", file=sys.stderr)
        print("  HUBSPOT_APP_ID", file=sys.stderr)
        print("  HUBSPOT_ACCESS_TOKEN", file=sys.stderr)
        sys.exit(1)

    manager = HubSpotWebhookManager(
        app_id=int(app_id),
        access_token=access_token,
        webhook_url=webhook_url or "",
    )

    # Execute command
    if args.list:
        print(f"Subscriptions for app {app_id}:\n")
        subscriptions = manager.list_subscriptions()
        if not subscriptions:
            print("  (none)")
        else:
            for sub in subscriptions:
                active = "✓" if sub.get("active") else "✗"
                prop = f" (property: {sub.get('propertyName')})" if sub.get("propertyName") else ""
                print(f"  {active} [{sub['id']}] {sub['eventType']}{prop}")
        print(f"\nTotal: {len(subscriptions)} subscription(s)")

    elif args.delete_all:
        confirm = input(
            f"Delete ALL webhook subscriptions for app {app_id}? (yes/no): "
        )
        if confirm.lower() == "yes":
            count = manager.delete_all_subscriptions()
            print(f"✓ Deleted {count} subscription(s)")
        else:
            print("Cancelled")

    elif args.object_type:
        if not webhook_url:
            print("Error: --webhook-url or WEBHOOK_URL env var required for registration", file=sys.stderr)
            sys.exit(1)

        print(f"Registering webhooks for {args.object_type}...")
        print(f"App ID: {app_id}")
        print(f"Webhook URL: {webhook_url}\n")

        results = manager.register_standard_events(args.object_type)
        print(f"\n✓ Registered {len(results)} webhook(s)")
        print("\nNext steps:")
        print("1. Start your webhook receiver: uvicorn main:app --port 8080")
        print("2. Start your event processor: python -m processor")
        print(f"3. Make changes to {args.object_type} objects in HubSpot")
        print("4. Watch the logs for incoming events!")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
