"""Tests unitaires pour GitHubWebhookParser."""
import hashlib
import hmac
import json
import pytest
from civitas_acquisition.connectors.code_repos.github.webhook import (
    GitHubWebhookParser, WebhookEvent,
)

SECRET = "my-webhook-secret"


def _sign(body: bytes, secret: str) -> str:
    sig = hmac.new(secret.encode(), msg=body, digestmod=hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def _headers(body: bytes, event_type: str, delivery_id: str = "abc-123", secret: str = SECRET) -> dict:
    return {
        "X-GitHub-Event": event_type,
        "X-GitHub-Delivery": delivery_id,
        "X-Hub-Signature-256": _sign(body, secret),
        "Content-Type": "application/json",
    }


def _push_payload(repo: str = "org/repo") -> dict:
    return {
        "ref": "refs/heads/main",
        "repository": {"full_name": repo},
        "sender": {"login": "alice"},
        "commits": [
            {"added": ["src/new_file.py"], "modified": ["README.md"], "removed": []},
        ],
    }


def _issue_payload(action: str = "opened", repo: str = "org/repo") -> dict:
    return {
        "action": action,
        "issue": {"number": 42, "html_url": f"https://github.com/{repo}/issues/42"},
        "repository": {"full_name": repo},
        "sender": {"login": "bob"},
    }


class TestWebhookSignature:

    def test_signature_valide_parse_ok(self):
        parser = GitHubWebhookParser(secret=SECRET)
        body = json.dumps(_push_payload()).encode()
        event = parser.parse(body=body, headers=_headers(body, "push"))
        assert event.event_type == "push"

    def test_signature_invalide_leve_value_error(self):
        parser = GitHubWebhookParser(secret=SECRET)
        body = json.dumps(_push_payload()).encode()
        headers = _headers(body, "push", secret="wrong-secret")
        with pytest.raises(ValueError, match="signature"):
            parser.parse(body=body, headers=headers)

    def test_sans_secret_pas_de_verification(self):
        parser = GitHubWebhookParser(secret=None)
        body = json.dumps(_push_payload()).encode()
        headers = {"X-GitHub-Event": "push", "X-GitHub-Delivery": "xyz"}
        event = parser.parse(body=body, headers=headers)
        assert event.event_type == "push"

    def test_header_signature_manquant_leve_error(self):
        parser = GitHubWebhookParser(secret=SECRET)
        body = json.dumps(_push_payload()).encode()
        headers = {"X-GitHub-Event": "push", "X-GitHub-Delivery": "xyz"}
        with pytest.raises(ValueError, match="Missing"):
            parser.parse(body=body, headers=headers)


class TestWebhookEventTypes:

    def _parse(self, payload: dict, event_type: str) -> WebhookEvent:
        parser = GitHubWebhookParser(secret=SECRET)
        body = json.dumps(payload).encode()
        return parser.parse(body=body, headers=_headers(body, event_type))

    def test_push_should_acquire(self):
        event = self._parse(_push_payload(), "push")
        assert event.should_acquire is True
        assert event.event_type == "push"

    def test_issues_opened_should_acquire(self):
        event = self._parse(_issue_payload("opened"), "issues")
        assert event.should_acquire is True

    def test_issues_labeled_should_not_acquire(self):
        event = self._parse(_issue_payload("labeled"), "issues")
        assert event.should_acquire is False

    def test_pr_opened_should_acquire(self):
        payload = {
            "action": "opened",
            "pull_request": {"html_url": "https://github.com/org/repo/pull/1"},
            "repository": {"full_name": "org/repo"},
            "sender": {"login": "carol"},
        }
        event = self._parse(payload, "pull_request")
        assert event.should_acquire is True

    def test_delivery_id_extrait(self):
        parser = GitHubWebhookParser(secret=SECRET)
        body = json.dumps(_push_payload()).encode()
        headers = _headers(body, "push", delivery_id="unique-delivery-id-789")
        event = parser.parse(body=body, headers=headers)
        assert event.delivery_id == "unique-delivery-id-789"

    def test_repo_full_name_extrait(self):
        event = self._parse(_push_payload("myorg/myrepo"), "push")
        assert event.repo_full_name == "myorg/myrepo"


class TestExtractFilesFromPush:

    def test_extract_files_added_et_modified(self):
        parser = GitHubWebhookParser()
        payload = {
            "ref": "refs/heads/main",
            "repository": {"full_name": "org/repo"},
            "sender": {"login": "alice"},
            "commits": [
                {"added": ["new.py", "docs/guide.md"], "modified": ["README.md"], "removed": ["old.txt"]},
            ],
        }
        body = json.dumps(payload).encode()
        headers = {"X-GitHub-Event": "push", "X-GitHub-Delivery": "xyz"}
        event = parser.parse(body=body, headers=headers)
        files = parser.extract_files_from_push(event)

        statuses = {f["path"]: f["status"] for f in files}
        assert statuses.get("new.py") == "added"
        assert statuses.get("docs/guide.md") == "added"
        assert statuses.get("README.md") == "modified"
        assert statuses.get("old.txt") == "removed"

    def test_extract_files_non_push_retourne_vide(self):
        parser = GitHubWebhookParser()
        body = json.dumps(_issue_payload()).encode()
        headers = {"X-GitHub-Event": "issues", "X-GitHub-Delivery": "xyz"}
        event = parser.parse(body=body, headers=headers)
        assert parser.extract_files_from_push(event) == []

    def test_affected_resources_push(self):
        payload = _push_payload()
        parser = GitHubWebhookParser()
        body = json.dumps(payload).encode()
        headers = {"X-GitHub-Event": "push", "X-GitHub-Delivery": "xyz"}
        event = parser.parse(body=body, headers=headers)
        resources = event.affected_resources
        assert any("README.md" in r for r in resources)
        assert any("new_file.py" in r for r in resources)
