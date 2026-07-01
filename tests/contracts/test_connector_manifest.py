"""Tests unitaires pour ConnectorManifest et les types associés."""

import pytest
from civitas_acquisition.contracts.models.connector_manifest import (
    ConnectorManifest,
    ChannelType,
    SourceCategory,
    RateLimit,
    CredentialSpec,
)


def make_github_manifest(**overrides) -> ConnectorManifest:
    defaults = dict(
        connector_id="github",
        display_name="GitHub",
        version="1.0.0",
        source_category=SourceCategory.CODE_REPOSITORY,
        supported_channels=frozenset([ChannelType.POLLING, ChannelType.WEBHOOK]),
        supported_mime_types=frozenset(["text/plain", "text/markdown", "application/json"]),
        required_credentials=(
            CredentialSpec(key="token", description="GitHub PAT", sensitive=True),
        ),
        rate_limit=RateLimit(requests_per_second=1.5, burst_size=10),
    )
    defaults.update(overrides)
    return ConnectorManifest(**defaults)


class TestConnectorManifestImmutabilite:

    def test_est_frozen(self):
        manifest = make_github_manifest()
        with pytest.raises((AttributeError, TypeError)):
            manifest.connector_id = "modified"  # type: ignore[misc]

    def test_supported_channels_est_frozenset(self):
        manifest = make_github_manifest()
        assert isinstance(manifest.supported_channels, frozenset)

    def test_supported_mime_types_est_frozenset(self):
        manifest = make_github_manifest()
        assert isinstance(manifest.supported_mime_types, frozenset)

    def test_required_credentials_est_tuple(self):
        manifest = make_github_manifest()
        assert isinstance(manifest.required_credentials, tuple)


class TestSupportsChannel:

    def test_channel_supporte_retourne_true(self):
        manifest = make_github_manifest()
        assert manifest.supports_channel(ChannelType.POLLING) is True
        assert manifest.supports_channel(ChannelType.WEBHOOK) is True

    def test_channel_non_supporte_retourne_false(self):
        manifest = make_github_manifest()
        assert manifest.supports_channel(ChannelType.STREAMING) is False
        assert manifest.supports_channel(ChannelType.QUEUE) is False
        assert manifest.supports_channel(ChannelType.FILE_DROP) is False
        assert manifest.supports_channel(ChannelType.MANUAL) is False

    def test_connecteur_uniquement_polling(self):
        manifest = make_github_manifest(
            supported_channels=frozenset([ChannelType.POLLING])
        )
        assert manifest.supports_channel(ChannelType.POLLING) is True
        assert manifest.supports_channel(ChannelType.WEBHOOK) is False


class TestSupportsMimeType:

    def test_mime_exact_supporte(self):
        manifest = make_github_manifest()
        assert manifest.supports_mime_type("text/plain") is True
        assert manifest.supports_mime_type("text/markdown") is True

    def test_mime_inconnu_non_supporte(self):
        manifest = make_github_manifest()
        assert manifest.supports_mime_type("video/mp4") is False
        assert manifest.supports_mime_type("audio/mpeg") is False

    def test_wildcard_total_accepte_tout(self):
        manifest = make_github_manifest(
            supported_mime_types=frozenset(["*/*"])
        )
        assert manifest.supports_mime_type("application/pdf") is True
        assert manifest.supports_mime_type("video/mp4") is True
        assert manifest.supports_mime_type("image/png") is True

    def test_wildcard_partiel(self):
        manifest = make_github_manifest(
            supported_mime_types=frozenset(["text/*"])
        )
        assert manifest.supports_mime_type("text/plain") is True
        assert manifest.supports_mime_type("text/html") is True
        assert manifest.supports_mime_type("application/pdf") is False


class TestCredentialKeys:

    def test_uniquement_required(self):
        manifest = make_github_manifest(
            required_credentials=(
                CredentialSpec(key="token", description="Token"),
            ),
            optional_credentials=(),
        )
        keys = manifest.all_credential_keys()
        assert "token" in keys
        assert len(keys) == 1

    def test_required_et_optional(self):
        manifest = make_github_manifest(
            required_credentials=(
                CredentialSpec(key="token", description="Token"),
                CredentialSpec(key="org", description="Organization"),
            ),
            optional_credentials=(
                CredentialSpec(key="proxy", description="Proxy URL", required=False),
            ),
        )
        keys = manifest.all_credential_keys()
        assert "token" in keys
        assert "org" in keys
        assert "proxy" in keys
        assert len(keys) == 3

    def test_sans_credentials(self):
        manifest = make_github_manifest(
            required_credentials=(),
            optional_credentials=(),
        )
        assert manifest.all_credential_keys() == ()


class TestValeursParlDefaut:

    def test_valeurs_par_defaut_raisonnables(self):
        manifest = make_github_manifest()
        assert manifest.max_batch_size == 100
        assert manifest.max_concurrency == 1
        assert manifest.supports_cursor is True
        assert manifest.supports_delta is False
        assert manifest.supports_streaming is False
        assert manifest.supports_discovery is True

    def test_optional_credentials_vide_par_defaut(self):
        manifest = make_github_manifest()
        assert manifest.optional_credentials == ()


class TestRateLimit:

    def test_rate_limit_immutable(self):
        rl = RateLimit(requests_per_second=1.5, burst_size=10)
        with pytest.raises((AttributeError, TypeError)):
            rl.requests_per_second = 5.0  # type: ignore[misc]

    def test_sans_rate_limit(self):
        manifest = make_github_manifest(rate_limit=None)
        assert manifest.rate_limit is None


class TestCredentialSpec:

    def test_sensible_par_defaut(self):
        spec = CredentialSpec(key="token", description="API Token")
        assert spec.sensitive is True
        assert spec.required is True

    def test_spec_optional(self):
        spec = CredentialSpec(key="proxy", description="Proxy", required=False)
        assert spec.required is False


class TestStr:

    def test_str_contient_connector_id(self):
        manifest = make_github_manifest()
        s = str(manifest)
        assert "github" in s
        assert "1.0.0" in s
