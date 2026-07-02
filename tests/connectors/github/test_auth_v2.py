"""Tests pour GitHubAuth v2 — PEM normalization, validate, cache multi-app."""
import pytest
import time
from civitas_acquisition.connectors.code_repos.github.auth import (
    GitHubAuth, _normalize_pem_key, _installation_token_cache,
)


class TestNormalizePemKey:

    def test_cle_deja_bien_formattee(self):
        key = "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIB...\n-----END RSA PRIVATE KEY-----"
        assert _normalize_pem_key(key) == key

    def test_newlines_echappes_convertis(self):
        key = "-----BEGIN RSA PRIVATE KEY-----\\nMIIEowIB\\nAQEFAASC\\n-----END RSA PRIVATE KEY-----"
        normalized = _normalize_pem_key(key)
        assert "\\n" not in normalized
        assert "\n" in normalized
        assert normalized.startswith("-----BEGIN RSA PRIVATE KEY-----")

    def test_strip_whitespace(self):
        key = "  -----BEGIN RSA PRIVATE KEY-----\nABC\n-----END RSA PRIVATE KEY-----  "
        normalized = _normalize_pem_key(key)
        assert not normalized.startswith(" ")
        assert not normalized.endswith(" ")

    def test_cle_vide(self):
        assert _normalize_pem_key("") == ""

    def test_cle_sans_header(self):
        # Clé sans header PEM — retournée telle quelle
        raw = "justrandombytes"
        assert _normalize_pem_key(raw) == raw


class TestGitHubAuthPAT:

    async def test_get_token_pat(self):
        auth = GitHubAuth.from_pat("ghp_token_123")
        assert await auth.get_token() == "ghp_token_123"

    async def test_get_token_oauth2(self):
        auth = GitHubAuth.from_oauth2("gho_oauth_456")
        assert await auth.get_token() == "gho_oauth_456"

    def test_is_not_app_auth(self):
        assert GitHubAuth.from_pat("ghp_xxx").is_app_auth is False

    def test_is_app_auth(self):
        auth = GitHubAuth.from_app("123", "-----BEGIN RSA PRIVATE KEY-----\nkey\n-----END RSA PRIVATE KEY-----", "456")
        assert auth.is_app_auth is True

    def test_repr_ne_expose_pas_token(self):
        auth = GitHubAuth.from_pat("ghp_super_secret")
        assert "super_secret" not in repr(auth)


class TestGitHubAppCacheKey:

    def test_cache_key_unique_par_app_installation(self):
        auth1 = GitHubAuth.from_app("app-1", "key", "inst-A")
        auth2 = GitHubAuth.from_app("app-1", "key", "inst-B")
        auth3 = GitHubAuth.from_app("app-2", "key", "inst-A")
        assert auth1._cache_key != auth2._cache_key
        assert auth1._cache_key != auth3._cache_key
        assert auth2._cache_key != auth3._cache_key

    def test_needs_refresh_token_absent(self):
        auth = GitHubAuth.from_app("app", "key", "inst")
        auth._token = ""
        assert auth._needs_refresh() is True

    def test_needs_refresh_token_frais(self):
        auth = GitHubAuth.from_app("app", "key", "inst")
        auth._token = "tok"
        _installation_token_cache[auth._cache_key] = {
            "token": "tok",
            "expires_at": time.time() + 3600,
        }
        assert auth._needs_refresh() is False

    def test_needs_refresh_token_expire_bientot(self):
        auth = GitHubAuth.from_app("app", "key", "inst-expire")
        auth._token = "tok"
        _installation_token_cache[auth._cache_key] = {
            "token": "tok",
            "expires_at": time.time() + 30,  # < leeway 60s
        }
        assert auth._needs_refresh() is True


class TestAuthHeader:

    def test_format_bearer(self):
        auth = GitHubAuth.from_pat("ghp_xxx")
        assert auth.auth_header("ghp_xxx") == "Bearer ghp_xxx"

    def test_format_app_token(self):
        auth = GitHubAuth.from_app("app", "key", "inst")
        assert auth.auth_header("ghs_install_token") == "Bearer ghs_install_token"
