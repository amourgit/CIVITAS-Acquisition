"""
GitHubAuth — gestion complète de l'authentification GitHub.

Supporte :
  - Personal Access Token (PAT) classique et fine-grained
  - GitHub App (JWT → Installation Token) avec cache par appId:installationId
  - OAuth2 Bearer Token

Améliorations vs v1 :
  - normalizePemKey() : gère les clés PEM avec \\n échappés (pattern Activepieces)
  - Cache keyed par appId:installationId (multi-app support)
  - validate() : vérifie les credentials avant connexion, fail-fast
  - Leeway configurable (défaut 60s) pour le refresh anticipé
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

JWT_CLOCK_SKEW_S  = 60    # 60s de tolérance horloge au moment de la signature
JWT_LIFETIME_S    = 540   # 9 minutes de durée de vie du JWT (max GitHub = 10min)
TOKEN_LEEWAY_S    = 60    # Rafraîchir 60s avant expiration

# Cache global : "appId:installationId" → {token, expires_at}
_installation_token_cache: dict[str, dict] = {}


def _normalize_pem_key(raw_key: str) -> str:
    """
    Normalise une clé PEM privée.

    Les utilisateurs collent souvent des clés avec \\n littéraux
    (depuis des variables d'environnement, des secrets CI, etc.).
    Cette fonction les convertit en vrais sauts de ligne.

    Adapté du pattern Activepieces auth-helpers.ts::normalizePemKey()
    """
    # Remplacer les \\n littéraux par de vrais sauts de ligne
    key = raw_key.replace("\\n", "\n").strip()

    # Si la clé a déjà des vrais sauts de ligne, on la retourne telle quelle
    if "\n" in key:
        return key

    # Sinon, tenter de reformater : extraire header, body, footer
    import re
    match = re.match(
        r"^(-----BEGIN [A-Z0-9 ]+-----)(.+?)(-----END [A-Z0-9 ]+-----)$",
        key,
    )
    if not match:
        return key

    header, body, footer = match.group(1), match.group(2), match.group(3)
    # Retirer tous les espaces du body et re-wrapper à 64 chars
    body_clean = re.sub(r"\s+", "", body)
    wrapped = "\n".join(
        body_clean[i:i+64] for i in range(0, len(body_clean), 64)
    )
    return f"{header}\n{wrapped}\n{footer}"


class GitHubAuth:
    """
    Gestionnaire d'authentification GitHub.

    Patterns supportés :
      auth = GitHubAuth.from_pat("ghp_xxxx")
      auth = GitHubAuth.from_oauth2("gho_xxxx")
      auth = GitHubAuth.from_app(app_id, private_key_pem, installation_id)
    """

    def __init__(
        self,
        token: str,
        token_type: str = "pat",
        app_id: Optional[str] = None,
        private_key: Optional[str] = None,
        installation_id: Optional[str] = None,
        leeway_s: int = TOKEN_LEEWAY_S,
    ) -> None:
        self._token = token
        self._token_type = token_type
        self._app_id = app_id
        self._private_key = _normalize_pem_key(private_key) if private_key else None
        self._installation_id = installation_id
        self._leeway_s = leeway_s

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_pat(cls, token: str) -> GitHubAuth:
        return cls(token=token, token_type="pat")

    @classmethod
    def from_oauth2(cls, access_token: str) -> GitHubAuth:
        return cls(token=access_token, token_type="oauth2")

    @classmethod
    def from_app(
        cls,
        app_id: str,
        private_key: str,
        installation_id: str,
        leeway_s: int = TOKEN_LEEWAY_S,
    ) -> GitHubAuth:
        return cls(
            token="",
            token_type="app",
            app_id=app_id,
            private_key=private_key,
            installation_id=installation_id,
            leeway_s=leeway_s,
        )

    # ── Token access ──────────────────────────────────────────────────────────

    async def get_token(self) -> str:
        if self._token_type in ("pat", "oauth2"):
            return self._token
        # GitHub App
        if self._needs_refresh():
            await self._refresh_app_token()
        return self._token

    def auth_header(self, token: str) -> str:
        return f"Bearer {token}"

    # ── Validation ────────────────────────────────────────────────────────────

    async def validate(self) -> tuple[bool, str]:
        """
        Vérifie les credentials avant la première connexion.
        Retourne (valid, error_message).

        Pattern Activepieces : auth.ts::validate()
        """
        try:
            if self._token_type == "app":
                # 1. Vérifier qu'on peut signer le JWT
                jwt_token = self._generate_jwt()
                # 2. Vérifier qu'on peut obtenir un installation token
                result = await self._fetch_installation_token(jwt_token)
                if "token" not in result:
                    return False, "Could not obtain installation token"
            else:
                # PAT / OAuth2 : vérifier via /user
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    headers = {
                        "Authorization": f"Bearer {self._token}",
                        "Accept": "application/vnd.github+json",
                    }
                    async with session.get(
                        "https://api.github.com/user", headers=headers
                    ) as resp:
                        if resp.status == 401:
                            return False, "Invalid token — 401 Unauthorized"
                        if resp.status not in (200, 304):
                            return False, f"Unexpected status {resp.status}"
            return True, ""
        except Exception as exc:
            return False, str(exc)

    # ── Internal ──────────────────────────────────────────────────────────────

    @property
    def _cache_key(self) -> str:
        return f"{self._app_id}:{self._installation_id}"

    def _needs_refresh(self) -> bool:
        cached = _installation_token_cache.get(self._cache_key, {})
        if not cached or not self._token:
            return True
        return time.time() >= (cached.get("expires_at", 0) - self._leeway_s)

    async def _refresh_app_token(self) -> None:
        jwt_token = self._generate_jwt()
        result = await self._fetch_installation_token(jwt_token)
        self._token = result["token"]
        # expires_at est ISO-8601 : "2024-01-15T10:00:00Z"
        expires_at_str = result.get("expires_at", "")
        if expires_at_str:
            from datetime import datetime, timezone
            dt = datetime.strptime(expires_at_str, "%Y-%m-%dT%H:%M:%SZ")
            expires_at = dt.replace(tzinfo=timezone.utc).timestamp()
        else:
            expires_at = time.time() + 3600
        _installation_token_cache[self._cache_key] = {
            "token": self._token,
            "expires_at": expires_at,
        }
        logger.info(
            "GitHub App token refreshed (app=%s, installation=%s)",
            self._app_id, self._installation_id,
        )

    def _generate_jwt(self) -> str:
        try:
            import jwt as pyjwt
        except ImportError:
            raise ImportError("PyJWT required: pip install 'PyJWT[crypto]'")
        now = int(time.time())
        payload = {
            "iat": now - JWT_CLOCK_SKEW_S,
            "exp": now + JWT_LIFETIME_S,
            "iss": self._app_id,
        }
        return pyjwt.encode(payload, self._private_key, algorithm="RS256")

    async def _fetch_installation_token(self, jwt_token: str) -> dict:
        import aiohttp
        url = (
            f"https://api.github.com/app/installations"
            f"/{self._installation_id}/access_tokens"
        )
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    raise RuntimeError(
                        f"Failed to obtain installation token ({resp.status}): {body}"
                    )
                return await resp.json()

    @property
    def is_app_auth(self) -> bool:
        return self._token_type == "app"

    def __repr__(self) -> str:
        return f"GitHubAuth(type={self._token_type!r}, token=<redacted>)"
