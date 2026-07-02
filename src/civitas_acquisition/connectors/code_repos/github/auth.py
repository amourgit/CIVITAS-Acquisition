"""
GitHubAuth — gestion de l'authentification GitHub.

Supporte :
  - Personal Access Token (PAT) : token statique, le plus courant
  - GitHub App : JWT → Installation Token (expire après 1h, auto-refresh)
  - Fine-grained PAT : même flow que PAT classique

Le token est encapsulé et jamais exposé dans les logs.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class GitHubAuth:
    """
    Gestionnaire d'authentification GitHub.

    Pour PAT :
        auth = GitHubAuth.from_pat("ghp_xxxx")

    Pour GitHub App :
        auth = GitHubAuth.from_app(
            app_id="123456",
            private_key="-----BEGIN RSA PRIVATE KEY-----\n...",
            installation_id="78901234",
        )
    """

    def __init__(
        self,
        token: str,
        token_type: str = "pat",
        app_id: Optional[str] = None,
        private_key: Optional[str] = None,
        installation_id: Optional[str] = None,
    ) -> None:
        self._token = token
        self._token_type = token_type
        self._app_id = app_id
        self._private_key = private_key
        self._installation_id = installation_id
        self._token_expires_at: Optional[float] = None

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_pat(cls, token: str) -> GitHubAuth:
        """Personal Access Token (classique ou fine-grained)."""
        return cls(token=token, token_type="pat")

    @classmethod
    def from_app(
        cls,
        app_id: str,
        private_key: str,
        installation_id: str,
    ) -> GitHubAuth:
        """
        GitHub App authentication.
        Le token d'installation est généré et rafraîchi automatiquement.
        """
        return cls(
            token="",   # sera rempli lors du premier get_token()
            token_type="app",
            app_id=app_id,
            private_key=private_key,
            installation_id=installation_id,
        )

    # ── Token access ──────────────────────────────────────────────────────────

    async def get_token(self) -> str:
        """
        Retourne le token actif.
        Pour GitHub App, rafraîchit si expiré (< 60s avant expiration).
        """
        if self._token_type == "pat":
            return self._token

        # GitHub App flow
        if self._needs_refresh():
            await self._refresh_app_token()

        return self._token

    def auth_header(self, token: str) -> str:
        return f"Bearer {token}"

    def _needs_refresh(self) -> bool:
        if not self._token:
            return True
        if self._token_expires_at is None:
            return True
        return time.time() >= (self._token_expires_at - 60)

    async def _refresh_app_token(self) -> None:
        """Génère un JWT GitHub App et l'échange contre un installation token."""
        try:
            jwt_token = self._generate_jwt()
            installation_token = await self._fetch_installation_token(jwt_token)
            self._token = installation_token["token"]
            # GitHub App tokens expirent après 1 heure
            self._token_expires_at = time.time() + 3600
            logger.info(
                "GitHub App token refreshed for installation %s",
                self._installation_id,
            )
        except Exception as exc:
            logger.error("Failed to refresh GitHub App token: %s", exc)
            raise

    def _generate_jwt(self) -> str:
        """Génère un JWT signé avec la clé privée de l'App."""
        try:
            import jwt as pyjwt
        except ImportError:
            raise ImportError(
                "PyJWT is required for GitHub App auth: pip install PyJWT cryptography"
            )

        now = int(time.time())
        payload = {
            "iat": now - 60,          # Issued at (60s skew tolérance)
            "exp": now + 600,          # Expiration : 10 minutes
            "iss": self._app_id,
        }
        return pyjwt.encode(payload, self._private_key, algorithm="RS256")

    async def _fetch_installation_token(self, jwt_token: str) -> dict:
        """
        Échange un JWT contre un installation access token.
        Appel direct sans passer par le GitHubClient (bootstrap problem).
        """
        import aiohttp
        url = f"https://api.github.com/app/installations/{self._installation_id}/access_tokens"
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()

    def __repr__(self) -> str:
        return f"GitHubAuth(type={self._token_type!r}, token=<redacted>)"
