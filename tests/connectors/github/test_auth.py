"""Tests unitaires pour GitHubAuth."""
import pytest
from civitas_acquisition.connectors.code_repos.github.auth import GitHubAuth


class TestGitHubAuthPAT:

    def test_from_pat_type_correct(self):
        auth = GitHubAuth.from_pat("ghp_xxxx")
        assert auth._token_type == "pat"

    async def test_get_token_retourne_pat(self):
        auth = GitHubAuth.from_pat("ghp_my_token")
        token = await auth.get_token()
        assert token == "ghp_my_token"

    def test_auth_header_format(self):
        auth = GitHubAuth.from_pat("ghp_xxxx")
        header = auth.auth_header("ghp_xxxx")
        assert header == "Bearer ghp_xxxx"

    def test_repr_ne_expose_pas_le_token(self):
        auth = GitHubAuth.from_pat("ghp_secret_token")
        r = repr(auth)
        assert "ghp_secret_token" not in r
        assert "redacted" in r.lower()


class TestGitHubAuthApp:

    def test_from_app_type_correct(self):
        auth = GitHubAuth.from_app(
            app_id="123456",
            private_key="-----BEGIN RSA PRIVATE KEY-----\n...",
            installation_id="78901234",
        )
        assert auth._token_type == "app"
        assert auth._app_id == "123456"
        assert auth._installation_id == "78901234"

    def test_needs_refresh_sans_token(self):
        auth = GitHubAuth.from_app("123", "key", "456")
        assert auth._needs_refresh() is True

    def test_needs_refresh_avec_token_frais(self):
        import time
        auth = GitHubAuth.from_app("123", "key", "456")
        auth._token = "some_token"
        auth._token_expires_at = time.time() + 3600  # expire dans 1h
        assert auth._needs_refresh() is False

    def test_needs_refresh_avec_token_expirant(self):
        import time
        auth = GitHubAuth.from_app("123", "key", "456")
        auth._token = "some_token"
        auth._token_expires_at = time.time() + 30  # expire dans 30s (< 60s buffer)
        assert auth._needs_refresh() is True
