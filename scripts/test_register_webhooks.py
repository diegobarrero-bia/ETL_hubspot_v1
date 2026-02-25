"""Tests for scripts/register_webhooks.py."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import yaml

from register_webhooks import load_config, list_subscriptions, register_subscriptions, HUBSPOT_API


# ── Fixtures ──

@pytest.fixture
def valid_config():
    return {
        "app_id": "12345",
        "webhook_url": "https://example.com/webhooks/hubspot",
        "subscriptions": [
            {
                "objectType": "contacts",
                "eventTypes": ["contact.creation", "contact.deletion"],
            },
            {
                "objectType": "deals",
                "eventTypes": ["deal.creation"],
            },
        ],
    }


@pytest.fixture
def valid_config_file(valid_config):
    """Write a valid config to a temp file and return its path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(valid_config, f)
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def headers():
    return {"Authorization": "Bearer test-token", "Content-Type": "application/json"}


# ── load_config ──

class TestLoadConfig:
    def test_loads_valid_config(self, valid_config_file, valid_config):
        result = load_config(valid_config_file)
        assert result["app_id"] == "12345"
        assert len(result["subscriptions"]) == 2

    def test_rejects_missing_subscriptions_key(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"app_id": "12345"}, f)
            path = f.name

        with pytest.raises(SystemExit):
            load_config(path)
        os.unlink(path)

    def test_rejects_placeholder_app_id(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({
                "app_id": "YOUR_HUBSPOT_APP_ID",
                "subscriptions": [{"objectType": "contacts", "eventTypes": ["contact.creation"]}],
            }, f)
            path = f.name

        with pytest.raises(SystemExit):
            load_config(path)
        os.unlink(path)

    def test_rejects_empty_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("")
            path = f.name

        with pytest.raises(SystemExit):
            load_config(path)
        os.unlink(path)


# ── list_subscriptions ──

class TestListSubscriptions:
    @patch("register_webhooks.requests.get")
    def test_returns_subscription_list(self, mock_get, headers):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": [
                {"id": "1", "eventType": "contact.creation", "active": True},
                {"id": "2", "eventType": "deal.creation", "active": False},
            ]},
        )

        result = list_subscriptions("12345", headers)

        assert len(result) == 2
        assert result[0]["eventType"] == "contact.creation"
        mock_get.assert_called_once_with(
            f"{HUBSPOT_API}/webhooks/v3/12345/subscriptions",
            headers=headers,
            timeout=30,
        )

    @patch("register_webhooks.requests.get")
    def test_returns_empty_on_error(self, mock_get, headers):
        mock_get.return_value = MagicMock(status_code=401, text="Unauthorized")

        result = list_subscriptions("12345", headers)
        assert result == []


# ── register_subscriptions ──

class TestRegisterSubscriptions:
    @patch("register_webhooks.requests.post")
    def test_creates_all_subscriptions(self, mock_post, valid_config, headers):
        mock_post.return_value = MagicMock(status_code=201)

        summary = register_subscriptions("12345", headers, valid_config)

        assert summary == {"created": 3, "exists": 0, "failed": 0}
        assert mock_post.call_count == 3  # 2 contacts + 1 deals

    @patch("register_webhooks.requests.post")
    def test_handles_existing_subscriptions(self, mock_post, valid_config, headers):
        mock_post.return_value = MagicMock(status_code=409)

        summary = register_subscriptions("12345", headers, valid_config)

        assert summary == {"created": 0, "exists": 3, "failed": 0}

    @patch("register_webhooks.requests.post")
    def test_handles_failures(self, mock_post, valid_config, headers):
        mock_post.return_value = MagicMock(status_code=500, text="Internal Server Error")

        summary = register_subscriptions("12345", headers, valid_config)

        assert summary == {"created": 0, "exists": 0, "failed": 3}

    @patch("register_webhooks.requests.post")
    def test_mixed_results(self, mock_post, valid_config, headers):
        """First call creates, second already exists, third fails."""
        mock_post.side_effect = [
            MagicMock(status_code=201),
            MagicMock(status_code=409),
            MagicMock(status_code=500, text="Error"),
        ]

        summary = register_subscriptions("12345", headers, valid_config)

        assert summary == {"created": 1, "exists": 1, "failed": 1}

    def test_dry_run_makes_no_api_calls(self, valid_config, headers):
        """Dry run should not call requests.post at all."""
        with patch("register_webhooks.requests.post") as mock_post:
            summary = register_subscriptions("12345", headers, valid_config, dry_run=True)

            mock_post.assert_not_called()
            assert summary["created"] == 3

    @patch("register_webhooks.requests.post")
    def test_sends_correct_payload(self, mock_post, headers):
        mock_post.return_value = MagicMock(status_code=201)

        config = {
            "subscriptions": [
                {"objectType": "contacts", "eventTypes": ["contact.creation"]},
            ],
        }

        register_subscriptions("12345", headers, config)

        mock_post.assert_called_once_with(
            f"{HUBSPOT_API}/webhooks/v3/12345/subscriptions",
            headers=headers,
            json={
                "eventType": "contact.creation",
                "propertyName": None,
                "active": True,
            },
            timeout=30,
        )
