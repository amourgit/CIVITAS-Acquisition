"""Tests unitaires pour le ConnectorRegistry."""

import pytest
from civitas_acquisition.registry.connector_registry import ConnectorRegistry
from civitas_acquisition.contracts.ports.connector_port import ConnectorPort
from civitas_acquisition.contracts.models.connector_manifest import (
    ConnectorManifest, ChannelType, SourceCategory,
)
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorNotFoundError,
    ManifestValidationError,
)


class FakeConnector(ConnectorPort):
    def manifest(self):
        return ConnectorManifest(
            connector_id="fake",
            display_name="Fake Source",
            version="1.0.0",
            source_category=SourceCategory.CUSTOM,
            supported_channels=frozenset([ChannelType.POLLING]),
            supported_mime_types=frozenset(["text/plain"]),
            required_credentials=(),
        )
    async def connect(self, config): pass
    async def disconnect(self): pass
    async def healthcheck(self): return HealthStatus.ok(10.0)
    async def discover(self): return DiscoveryResult(resources=(), total=0)
    async def pull(self, cursor=None, batch_size=100):
        return; yield


class AnotherConnector(ConnectorPort):
    def manifest(self):
        return ConnectorManifest(
            connector_id="another",
            display_name="Another Source",
            version="2.0.0",
            source_category=SourceCategory.WEB,
            supported_channels=frozenset([ChannelType.POLLING, ChannelType.WEBHOOK]),
            supported_mime_types=frozenset(["application/json"]),
            required_credentials=(),
        )
    async def connect(self, config): pass
    async def disconnect(self): pass
    async def healthcheck(self): return HealthStatus.ok(5.0)
    async def discover(self): return DiscoveryResult(resources=(), total=0)
    async def pull(self, cursor=None, batch_size=100):
        return; yield


class TestConnectorRegistry:

    @pytest.fixture
    def registry(self):
        return ConnectorRegistry()

    def test_register_et_get(self, registry):
        registry.register(FakeConnector)
        cls = registry.get("fake")
        assert cls is FakeConnector

    def test_is_registered(self, registry):
        assert registry.is_registered("fake") is False
        registry.register(FakeConnector)
        assert registry.is_registered("fake") is True

    def test_get_inconnu_leve_not_found(self, registry):
        with pytest.raises(ConnectorNotFoundError) as exc_info:
            registry.get("unknown")
        assert "unknown" in str(exc_info.value)

    def test_count(self, registry):
        assert registry.count() == 0
        registry.register(FakeConnector)
        assert registry.count() == 1
        registry.register(AnotherConnector)
        assert registry.count() == 2

    def test_register_meme_id_deux_fois_idempotent(self, registry):
        registry.register(FakeConnector)
        registry.register(FakeConnector)  # pas d'erreur
        assert registry.count() == 1

    def test_list_all(self, registry):
        registry.register(FakeConnector)
        registry.register(AnotherConnector)
        manifests = registry.list_all()
        ids = [m.connector_id for m in manifests]
        assert "fake" in ids
        assert "another" in ids

    def test_find_by_channel_polling(self, registry):
        registry.register(FakeConnector)
        registry.register(AnotherConnector)
        polling = registry.find_by_channel(ChannelType.POLLING)
        ids = [m.connector_id for m in polling]
        assert "fake" in ids
        assert "another" in ids

    def test_find_by_channel_webhook(self, registry):
        registry.register(FakeConnector)
        registry.register(AnotherConnector)
        webhook = registry.find_by_channel(ChannelType.WEBHOOK)
        ids = [m.connector_id for m in webhook]
        assert "another" in ids
        assert "fake" not in ids

    def test_find_by_category(self, registry):
        registry.register(FakeConnector)
        registry.register(AnotherConnector)
        web = registry.find_by_category(SourceCategory.WEB)
        assert any(m.connector_id == "another" for m in web)

    def test_manifest_direct(self, registry):
        registry.register(FakeConnector)
        m = registry.manifest("fake")
        assert m.connector_id == "fake"
        assert m.version == "1.0.0"

    def test_autodiscover_package_inexistant_retourne_zero(self, registry):
        count = registry.autodiscover("nonexistent.package.xyz")
        assert count == 0
