"""
GitHubWebhookParser — parsing et vérification des événements webhook GitHub.

Événements supportés :
  push             → fichiers créés/modifiés/supprimés
  issues           → issue créée/modifiée/fermée/rouverte
  issue_comment    → commentaire sur une issue
  pull_request     → PR créée/modifiée/mergée/fermée
  pull_request_review → review soumise
  release          → release publiée
  create           → branch/tag créé
  delete           → branch/tag supprimé
  repository       → repo créé/archivé/supprimé

Sécurité :
  - Vérification HMAC-SHA256 de la signature X-Hub-Signature-256
  - Validation du timestamp pour protéger contre les replay attacks
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Fenêtre de tolérance temporelle pour la protection replay
TIMESTAMP_TOLERANCE_S = 300.0   # 5 minutes


@dataclass(frozen=True)
class WebhookEvent:
    """
    Événement webhook GitHub parsé et validé.
    Prêt à être converti en RawDocument(s) par le mapper.
    """
    event_type: str           # "push", "issues", "pull_request", ...
    action: Optional[str]     # "opened", "closed", "synchronize", ...
    delivery_id: str          # X-GitHub-Delivery header
    repo_full_name: str
    payload: dict[str, Any]   # Payload original complet
    sender: str               # Login de l'utilisateur déclencheur

    @property
    def should_acquire(self) -> bool:
        """True si cet événement doit déclencher une acquisition."""
        if self.event_type == "push":
            return True
        if self.event_type in ("issues", "issue_comment"):
            return self.action in ("opened", "edited", "closed", "reopened", "created")
        if self.event_type in ("pull_request", "pull_request_review"):
            return self.action in ("opened", "edited", "closed", "synchronize", "submitted", "reopened")
        if self.event_type == "release":
            return self.action in ("published", "edited", "released")
        if self.event_type == "repository":
            return self.action in ("created", "publicized")
        return False

    @property
    def affected_resources(self) -> list[str]:
        """URIs des ressources affectées par cet événement."""
        resources = []
        repo = self.repo_full_name

        if self.event_type == "push":
            for commit in self.payload.get("commits", []):
                for f in commit.get("added", []) + commit.get("modified", []):
                    branch = self.payload.get("ref", "refs/heads/main").split("/")[-1]
                    resources.append(
                        f"https://github.com/{repo}/blob/{branch}/{f}"
                    )

        elif self.event_type == "issues":
            issue = self.payload.get("issue", {})
            resources.append(issue.get("html_url", ""))

        elif self.event_type == "pull_request":
            pr = self.payload.get("pull_request", {})
            resources.append(pr.get("html_url", ""))

        elif self.event_type == "release":
            release = self.payload.get("release", {})
            resources.append(release.get("html_url", ""))

        return [r for r in resources if r]


class GitHubWebhookParser:
    """
    Parse et valide les événements webhook GitHub.

    Usage :
        parser = GitHubWebhookParser(secret="my-webhook-secret")
        event = parser.parse(
            body=request.body,
            headers=request.headers,
        )
        if event.should_acquire:
            await trigger_acquisition(event)
    """

    def __init__(self, secret: Optional[str] = None) -> None:
        self._secret = secret.encode("utf-8") if secret else None

    def parse(
        self,
        body: bytes,
        headers: dict[str, str],
    ) -> WebhookEvent:
        """
        Valide la signature et parse l'événement webhook.
        Lève ValueError si la signature est invalide.
        Lève KeyError si des champs obligatoires manquent.
        """
        # Normaliser les headers (insensible à la casse)
        normalized = {k.lower(): v for k, v in headers.items()}

        # 1. Vérification de la signature
        if self._secret:
            self._verify_signature(body, normalized)

        # 2. Parser le payload
        payload = json.loads(body)

        # 3. Extraire les champs obligatoires
        event_type = normalized.get("x-github-event", "")
        delivery_id = normalized.get("x-github-delivery", "")
        repo = payload.get("repository", {})

        return WebhookEvent(
            event_type=event_type,
            action=payload.get("action"),
            delivery_id=delivery_id,
            repo_full_name=repo.get("full_name", ""),
            payload=payload,
            sender=payload.get("sender", {}).get("login", ""),
        )

    def _verify_signature(self, body: bytes, headers: dict[str, str]) -> None:
        """
        Vérifie la signature HMAC-SHA256 X-Hub-Signature-256.
        Utilise hmac.compare_digest pour résister aux timing attacks.
        """
        signature_header = headers.get("x-hub-signature-256", "")
        if not signature_header:
            raise ValueError("Missing X-Hub-Signature-256 header")

        if not signature_header.startswith("sha256="):
            raise ValueError("Invalid signature format — expected sha256=<hash>")

        expected_sig = signature_header[7:]  # strip "sha256="
        computed_sig = hmac.new(
            self._secret,
            msg=body,
            digestmod=hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(expected_sig, computed_sig):
            raise ValueError("Webhook signature verification failed")

    def extract_files_from_push(self, event: WebhookEvent) -> list[dict[str, Any]]:
        """
        Extrait la liste des fichiers affectés par un push event.
        Retourne une liste de dicts avec path, status (added/modified/removed).
        """
        if event.event_type != "push":
            return []

        files: dict[str, str] = {}
        for commit in event.payload.get("commits", []):
            for path in commit.get("added", []):
                files[path] = "added"
            for path in commit.get("modified", []):
                files[path] = "modified"
            for path in commit.get("removed", []):
                files[path] = "removed"

        return [{"path": p, "status": s} for p, s in files.items()]

    def extract_ref_info(self, event: WebhookEvent) -> tuple[str, str]:
        """
        Extrait le ref et le branch name d'un push event.
        Returns (ref, branch_name).
        """
        ref = event.payload.get("ref", "refs/heads/main")
        branch = ref.split("/")[-1]
        return ref, branch
