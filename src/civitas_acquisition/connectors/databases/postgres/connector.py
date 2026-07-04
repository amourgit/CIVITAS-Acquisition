"""
PostgreSQLConnector — connecteur PostgreSQL complet.

Deux modes d'acquisition :
  1. SNAPSHOT  : lecture complète ou partielle via SELECT
  2. CDC       : Change Data Capture via logical replication (pgoutput)

CDC est le mode recommandé pour les grosses tables (millions de lignes).
Il utilise pg_logical_replication pour capturer INSERT/UPDATE/DELETE en temps réel
sans scanner la table entière à chaque cycle.

Canaux :
  POLLING  → snapshot périodique (SELECT avec cursor sur colonne updated_at)
  STREAMING → CDC via logical replication slot (nécessite PostgreSQL ≥ 10)
  MANUAL   → snapshot one-shot

Config options :
  mode           : str        — "snapshot" | "cdc" (défaut: "snapshot")
  tables         : list[str]  — tables à acquérir (ex: ["users", "orders"])
  schema         : str        — schéma PostgreSQL (défaut: "public")
  cursor_column  : str        — colonne de cursor pour snapshot (défaut: "updated_at")
  batch_size     : int        — lignes par batch (défaut: 1000)
  max_rows       : int        — max rows par table par cycle (défaut: 100000)
  replication_slot : str      — nom du slot de réplication pour CDC
  publication    : str        — nom de la publication CDC
  include_schema : bool       — inclure le schéma dans les métadonnées (défaut: True)
  row_format     : str        — "json" | "csv" (défaut: "json")

Credentials :
  host     : hôte PostgreSQL
  port     : port (défaut: 5432)
  database : nom de la base
  username : utilisateur
  password : mot de passe
  sslmode  : "disable" | "require" | "verify-full" (défaut: "require")
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

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
    ConnectorAuthenticationError, ConnectorNetworkError, ConnectorFatalError,
)

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="postgresql",
            display_name="PostgreSQL",
            version="1.0.0",
            source_category=SourceCategory.DATABASE,
            supported_channels=frozenset([
                ChannelType.POLLING,
                ChannelType.STREAMING,
                ChannelType.MANUAL,
            ]),
            supported_mime_types=frozenset(["application/json", "text/csv"]),
            required_credentials=(
                CredentialSpec(key="host",     description="Hôte PostgreSQL"),
                CredentialSpec(key="database", description="Nom de la base de données"),
                CredentialSpec(key="username", description="Utilisateur PostgreSQL"),
                CredentialSpec(key="password", description="Mot de passe", sensitive=True),
            ),
            optional_credentials=(
                CredentialSpec(key="port",    description="Port (défaut 5432)",    required=False, sensitive=False),
                CredentialSpec(key="sslmode", description="SSL mode",              required=False, sensitive=False),
            ),
            rate_limit=RateLimit(requests_per_second=100.0, burst_size=500),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    # ── Connect ───────────────────────────────────────────────────────────────

    async def _do_connect(self, config: ConnectorConfig) -> None:
        try:
            import asyncpg
        except ImportError:
            raise ImportError("asyncpg required: pip install asyncpg")

        self._asyncpg = asyncpg
        dsn = self._build_dsn(config)

        try:
            self._pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=1,
                max_size=config.get_option("pool_size", 5),
                command_timeout=config.get_option("query_timeout_s", 60.0),
            )
        except Exception as exc:
            if "password authentication" in str(exc) or "authentication" in str(exc).lower():
                raise ConnectorAuthenticationError("postgresql", str(exc)) from exc
            raise ConnectorNetworkError("postgresql", cause=str(exc)) from exc

        self._schema       = config.get_option("schema", "public")
        self._tables       = config.get_option("tables", [])
        self._cursor_col   = config.get_option("cursor_column", "updated_at")
        self._batch_size   = config.get_option("batch_size", 1000)
        self._max_rows     = config.get_option("max_rows", 100_000)
        self._mode         = config.get_option("mode", "snapshot")
        self._include_schema = config.get_option("include_schema", True)
        self._row_format   = config.get_option("row_format", "json")
        self._replication_slot = config.get_option("replication_slot", "civitas_slot")
        self._publication  = config.get_option("publication", "civitas_publication")

        logger.info("PostgreSQL connected: %s/%s (mode=%s)", config.get_credential("host"), config.get_credential("database"), self._mode)

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_pool") and self._pool:
            await self._pool.close()

    # ── Health ────────────────────────────────────────────────────────────────

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            async with self._pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                row_counts = {}
                for table in self._tables[:5]:
                    n = await conn.fetchval(
                        f"SELECT reltuples::bigint FROM pg_class WHERE relname = $1", table
                    )
                    row_counts[table] = n or 0
            return HealthStatus.ok(
                latency_ms=(time.monotonic() - start) * 1000,
                version=version,
                tables=row_counts,
            )
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def discover(self) -> DiscoveryResult:
        """Liste toutes les tables du schéma avec colonnes et estimations."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    t.table_name,
                    obj_description(pgc.oid, 'pg_class') AS table_comment,
                    pg_total_relation_size(pgc.oid) AS size_bytes,
                    pgc.reltuples::bigint AS estimated_rows
                FROM information_schema.tables t
                JOIN pg_class pgc ON pgc.relname = t.table_name
                WHERE t.table_schema = $1
                  AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name
            """, self._schema)

        resources = tuple(
            f"postgresql://{self._schema}/{r['table_name']}" for r in rows
        )
        return DiscoveryResult(
            resources=resources,
            total=len(resources),
            metadata={
                r["table_name"]: {
                    "estimated_rows": r["estimated_rows"],
                    "size_bytes": r["size_bytes"],
                    "comment": r["table_comment"],
                }
                for r in rows
            },
        )

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors = json.loads(cursor.value) if cursor else {}
        updated = dict(cursors)
        count   = 0

        tables = self._tables
        if not tables:
            tables = await self._list_tables()

        for table in tables:
            if count >= batch_size:
                break

            # Récupérer le schéma de la table si activé
            table_schema = {}
            if self._include_schema:
                table_schema = await self._get_table_schema(table)

            since = cursors.get(f"{self._schema}.{table}")
            rows_fetched = 0

            async for batch in self._fetch_table_batches(table, since=since):
                for row in batch:
                    if count >= batch_size or rows_fetched >= self._max_rows:
                        break

                    doc = self._map_row(row, table, table_schema)
                    # Avancer le cursor sur la colonne définie
                    if self._cursor_col in row:
                        val = row[self._cursor_col]
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        updated[f"{self._schema}.{table}"] = str(val)

                    doc = self._stamp(doc, updated)
                    yield doc
                    count += 1
                    rows_fetched += 1

                if rows_fetched >= self._max_rows:
                    break

        logger.info("PostgreSQL pull completed: %d documents", count)

    # ── Snapshot batches ──────────────────────────────────────────────────────

    async def _fetch_table_batches(
        self, table: str, since: Optional[str] = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Lit une table par batches avec cursor sur cursor_column."""
        offset = 0
        has_cursor_col = await self._column_exists(table, self._cursor_col)

        while True:
            async with self._pool.acquire() as conn:
                if has_cursor_col and since:
                    query = f"""
                        SELECT * FROM {self._schema}.{table}
                        WHERE {self._cursor_col} > $1
                        ORDER BY {self._cursor_col} ASC
                        LIMIT $2 OFFSET $3
                    """
                    rows = await conn.fetch(query, since, self._batch_size, offset)
                else:
                    query = f"""
                        SELECT * FROM {self._schema}.{table}
                        ORDER BY ctid
                        LIMIT $1 OFFSET $2
                    """
                    rows = await conn.fetch(query, self._batch_size, offset)

            if not rows:
                break

            yield [dict(r) for r in rows]
            offset += len(rows)

            if len(rows) < self._batch_size:
                break

    # ── CDC via logical replication ───────────────────────────────────────────

    async def setup_cdc(self) -> bool:
        """
        Configure la réplication logique pour le CDC.
        Crée la publication et le slot si inexistants.
        Nécessite SUPERUSER ou pg_create_replication_slot.
        """
        async with self._pool.acquire() as conn:
            # Vérifier que le WAL level est correct
            wal_level = await conn.fetchval("SHOW wal_level")
            if wal_level not in ("logical",):
                logger.error("PostgreSQL wal_level must be 'logical', got '%s'", wal_level)
                return False

            # Créer la publication si elle n'existe pas
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_publication WHERE pubname = $1",
                self._publication,
            )
            if not exists:
                tables_clause = ""
                if self._tables:
                    table_list = ", ".join(
                        f"{self._schema}.{t}" for t in self._tables
                    )
                    tables_clause = f"FOR TABLE {table_list}"
                else:
                    tables_clause = f"FOR ALL TABLES IN SCHEMA {self._schema}"

                await conn.execute(
                    f"CREATE PUBLICATION {self._publication} {tables_clause}"
                )
                logger.info("Created publication: %s", self._publication)

            # Créer le slot de réplication si inexistant
            slot_exists = await conn.fetchval(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                self._replication_slot,
            )
            if not slot_exists:
                await conn.execute(
                    "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                    self._replication_slot,
                )
                logger.info("Created replication slot: %s", self._replication_slot)

        return True

    async def stream_cdc_changes(
        self, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        """
        Stream des changements CDC depuis le slot de réplication.
        Retourne des RawDocuments pour chaque INSERT/UPDATE/DELETE.

        Note : utilise pg_logical_slot_get_changes en mode polling léger.
        Pour un vrai streaming temps-réel, utiliser le protocole de replication
        asyncpg avec create_connection(dsn, replication="logical").
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT lsn, xid, data
                FROM pg_logical_slot_get_changes(
                    $1, NULL, $2,
                    'publication_names', $3,
                    'proto_version', '1'
                )
                """,
                self._replication_slot,
                batch_size,
                self._publication,
            )

        for row in rows:
            try:
                change = self._parse_pgoutput(row["data"], row["lsn"])
                if change:
                    yield change
            except Exception as exc:
                logger.debug("CDC parse error: %s", exc)

    def _parse_pgoutput(self, data: str, lsn: Any) -> Optional[RawDocument]:
        """Parse un message pgoutput en RawDocument."""
        try:
            payload = json.loads(data) if data.startswith("{") else {"raw": data}
        except Exception:
            payload = {"raw": data, "lsn": str(lsn)}

        action = payload.get("action", "unknown")
        table  = payload.get("table", "unknown")

        content = json.dumps(
            {"lsn": str(lsn), "change": payload},
            ensure_ascii=False,
        ).encode()

        return RawDocument.create(
            instance_id=self.instance_id,
            connector_id="postgresql",
            uri=f"postgresql://cdc/{table}/{lsn}",
            content=content,
            content_type="application/json",
            version=str(lsn),
            cursor=Cursor(
                value=str(lsn),
                source_type="sequence",
                connector_id="postgresql",
                instance_id=self.instance_id,
            ),
            tags=("cdc", f"table:{table}", f"action:{action}"),
            source_metadata={
                "resource_type": "cdc_change",
                "table": table,
                "action": action,
                "lsn": str(lsn),
            },
        )

    # ── Row mapping ───────────────────────────────────────────────────────────

    def _map_row(
        self, row: dict[str, Any], table: str, schema: dict,
    ) -> RawDocument:
        """Mappe une ligne de table en RawDocument."""
        serializable = self._make_serializable(row)
        content = json.dumps(serializable, ensure_ascii=False, default=str).encode()

        # Trouver une clé primaire ou identifiant
        pk_val = self._extract_pk(row, schema)
        uri = f"postgresql://{self._schema}/{table}/{pk_val}"

        version = None
        if self._cursor_col in row and row[self._cursor_col] is not None:
            v = row[self._cursor_col]
            version = v.isoformat() if isinstance(v, datetime) else str(v)

        return RawDocument.create(
            instance_id=self.instance_id,
            connector_id="postgresql",
            uri=uri,
            content=content,
            content_type="application/json",
            version=version,
            tags=("row", f"table:{table}", f"schema:{self._schema}"),
            source_metadata={
                "resource_type": "table_row",
                "schema": self._schema,
                "table": table,
                "pk": pk_val,
                "columns": list(row.keys()),
            },
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _list_tables(self) -> list[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = $1 AND table_type = 'BASE TABLE'
                   ORDER BY table_name""",
                self._schema,
            )
        return [r["table_name"] for r in rows]

    async def _get_table_schema(self, table: str) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            cols = await conn.fetch(
                """SELECT column_name, data_type, is_nullable, column_default
                   FROM information_schema.columns
                   WHERE table_schema = $1 AND table_name = $2
                   ORDER BY ordinal_position""",
                self._schema, table,
            )
            pks = await conn.fetch(
                """SELECT kcu.column_name
                   FROM information_schema.table_constraints tc
                   JOIN information_schema.key_column_usage kcu
                     ON tc.constraint_name = kcu.constraint_name
                   WHERE tc.table_schema = $1 AND tc.table_name = $2
                     AND tc.constraint_type = 'PRIMARY KEY'""",
                self._schema, table,
            )
        return {
            "columns": {r["column_name"]: {"type": r["data_type"], "nullable": r["is_nullable"] == "YES"} for r in cols},
            "primary_keys": [r["column_name"] for r in pks],
        }

    async def _column_exists(self, table: str, column: str) -> bool:
        async with self._pool.acquire() as conn:
            result = await conn.fetchval(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema=$1 AND table_name=$2 AND column_name=$3""",
                self._schema, table, column,
            )
        return result is not None

    def _extract_pk(self, row: dict, schema: dict) -> str:
        pks = schema.get("primary_keys", [])
        if pks:
            return "|".join(str(row.get(pk, "")) for pk in pks)
        for candidate in ("id", "uuid", "pk", "key"):
            if candidate in row:
                return str(row[candidate])
        return str(hash(frozenset(str(v) for v in list(row.values())[:3])))

    def _make_serializable(self, row: dict) -> dict:
        result = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, (bytes, bytearray, memoryview)):
                result[k] = "<binary>"
            elif hasattr(v, "__class__") and v.__class__.__name__ == "Decimal":
                result[k] = float(v)
            else:
                result[k] = v
        return result

    def _stamp(self, doc: RawDocument, cursors: dict) -> RawDocument:
        import dataclasses
        return dataclasses.replace(
            doc,
            cursor=Cursor(
                value=json.dumps(cursors, sort_keys=True),
                source_type="token",
                connector_id="postgresql",
                instance_id=self.instance_id,
            ),
        )

    @staticmethod
    def _build_dsn(config: ConnectorConfig) -> str:
        host     = config.get_credential("host")
        database = config.get_credential("database")
        username = config.get_credential("username")
        password = config.get_credential("password")
        port     = config.credentials.get("port", "5432")
        sslmode  = config.credentials.get("sslmode", "require")
        return f"postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
