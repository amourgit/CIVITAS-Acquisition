"""
S3Connector — connecteur AWS S3 / MinIO / GCS (compatible S3) complet.

Supporte :
  - AWS S3 natif
  - MinIO (endpoint_url personnalisé)
  - GCS via l'API de compatibilité S3
  - Tout stockage objet compatible S3

Config options :
  bucket         : str       — nom du bucket obligatoire
  prefix         : str       — préfixe de filtrage des clés (ex: "docs/")
  endpoint_url   : str       — pour MinIO/GCS (ex: "http://minio:9000")
  region_name    : str       — région AWS (défaut: us-east-1)
  file_extensions: list[str] — extensions à acquérir (défaut: toutes)
  max_file_size  : int       — taille max en bytes (défaut: 50MB)
  include_versions: bool     — inclure les versions d'objets (défaut: False)

Credentials :
  access_key_id     : AWS Access Key ID
  secret_access_key : AWS Secret Access Key
  session_token     : (optionnel) pour credentials temporaires

Cursor : ETag ou LastModified ISO-8601 du dernier objet traité.
"""
from __future__ import annotations
import logging
import mimetypes
import time
from typing import AsyncIterator, Optional
from civitas_acquisition.connectors._base import BaseConnector
from civitas_acquisition.contracts.models.connector_config import ConnectorConfig
from civitas_acquisition.contracts.models.connector_manifest import (
    ChannelType, ConnectorManifest, CredentialSpec, RateLimit, SourceCategory,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.discovery_result import DiscoveryResult
from civitas_acquisition.contracts.models.health_status import HealthStatus
from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.errors.connector_errors import (
    ConnectorAuthenticationError, ConnectorNetworkError,
)

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
EXCLUDED_EXTENSIONS = frozenset([".tmp", ".log", ".lock", ".pyc"])


def _detect_mime(key: str) -> str:
    mime, _ = mimetypes.guess_type(key)
    return mime or "application/octet-stream"


class S3Connector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="s3",
            display_name="AWS S3 / MinIO / GCS",
            version="1.0.0",
            source_category=SourceCategory.CLOUD_STORAGE,
            supported_channels=frozenset([
                ChannelType.POLLING, ChannelType.FILE_DROP, ChannelType.MANUAL,
            ]),
            supported_mime_types=frozenset(["*/*"]),
            required_credentials=(
                CredentialSpec(key="access_key_id", description="AWS Access Key ID"),
                CredentialSpec(key="secret_access_key", description="AWS Secret Access Key"),
            ),
            optional_credentials=(
                CredentialSpec(key="session_token", description="AWS Session Token (STS)", required=False),
            ),
            rate_limit=RateLimit(requests_per_second=50.0, burst_size=200),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    async def _do_connect(self, config: ConnectorConfig) -> None:
        try:
            import boto3
            import botocore
        except ImportError:
            raise ImportError("boto3 required for S3 connector: pip install boto3")

        creds = {
            "aws_access_key_id": config.get_credential("access_key_id"),
            "aws_secret_access_key": config.get_credential("secret_access_key"),
        }
        if token := config.credentials.get("session_token"):
            creds["aws_session_token"] = token

        session_kwargs: dict = {
            "region_name": config.get_option("region_name", "us-east-1"),
        }

        import asyncio
        import concurrent.futures
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        # boto3 est synchrone — on l'utilise dans un executor
        import boto3
        client_kwargs = {**creds, **session_kwargs}
        if endpoint := config.get_option("endpoint_url"):
            client_kwargs["endpoint_url"] = endpoint

        self._s3 = boto3.client("s3", **client_kwargs)
        self._bucket = config.get_option("bucket", "")
        if not self._bucket:
            raise ConnectorAuthenticationError("s3", "bucket option is required")

        self._prefix = config.get_option("prefix", "")
        self._max_size = config.get_option("max_file_size", MAX_FILE_SIZE)
        self._extensions = config.get_option("file_extensions", [])
        self._loop = asyncio.get_event_loop()

        # Vérifier l'accès
        await self._run_sync(self._s3.head_bucket, Bucket=self._bucket)
        logger.info("S3 connected: s3://%s/%s", self._bucket, self._prefix)

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_executor"):
            self._executor.shutdown(wait=False)

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            await self._run_sync(self._s3.head_bucket, Bucket=self._bucket)
            return HealthStatus.ok(latency_ms=(time.monotonic() - start) * 1000,
                                   bucket=self._bucket)
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    async def discover(self) -> DiscoveryResult:
        """Liste les préfixes (dossiers) au niveau racine du bucket."""
        try:
            resp = await self._run_sync(
                self._s3.list_objects_v2,
                Bucket=self._bucket,
                Prefix=self._prefix,
                Delimiter="/",
            )
            prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
            return DiscoveryResult(
                resources=tuple(f"s3://{self._bucket}/{p}" for p in prefixes),
                total=len(prefixes),
            )
        except Exception as exc:
            raise ConnectorNetworkError("s3", cause=str(exc))

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        since = cursor.value if cursor else None
        count = 0
        paginator = self._s3.get_paginator("list_objects_v2")

        pages_iter = paginator.paginate(
            Bucket=self._bucket,
            Prefix=self._prefix,
            PaginationConfig={"MaxItems": batch_size * 10, "PageSize": 1000},
        )

        async for page in self._run_paginator(pages_iter):
            for obj in page.get("Contents", []):
                if count >= batch_size:
                    return

                key = obj["Key"]
                size = obj.get("Size", 0)
                last_modified = obj["LastModified"].isoformat()
                etag = obj.get("ETag", "").strip('"')

                # Filtres
                if size == 0 or size > self._max_size:
                    continue
                ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
                if ext in EXCLUDED_EXTENSIONS:
                    continue
                if self._extensions and ext not in self._extensions:
                    continue

                # Delta : ne retraiter que les objets modifiés après le cursor
                if since and last_modified <= since:
                    continue

                # Récupérer le contenu
                try:
                    content = await self._fetch_object(key)
                except Exception as exc:
                    logger.warning("Error fetching s3://%s/%s: %s", self._bucket, key, exc)
                    continue

                uri = f"s3://{self._bucket}/{key}"
                yield RawDocument.create(
                    instance_id=self.instance_id,
                    connector_id="s3",
                    uri=uri,
                    content=content,
                    content_type=_detect_mime(key),
                    version=etag,
                    cursor=Cursor(
                        value=last_modified,
                        source_type="timestamp",
                        connector_id="s3",
                        instance_id=self.instance_id,
                    ),
                    tags=("s3", f"bucket:{self._bucket}"),
                    source_metadata={
                        "bucket": self._bucket,
                        "key": key,
                        "size": size,
                        "etag": etag,
                        "last_modified": last_modified,
                        "prefix": self._prefix,
                    },
                )
                count += 1

    async def _fetch_object(self, key: str) -> bytes:
        resp = await self._run_sync(
            self._s3.get_object, Bucket=self._bucket, Key=key
        )
        return resp["Body"].read()

    async def _run_sync(self, fn, **kwargs):
        """Exécute une fonction boto3 synchrone dans l'executor."""
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(**kwargs))

    async def _run_paginator(self, pages_iter):
        """Itère sur les pages boto3 dans l'executor."""
        import asyncio
        loop = asyncio.get_event_loop()
        pages = await loop.run_in_executor(self._executor, lambda: list(pages_iter))
        for page in pages:
            yield page
