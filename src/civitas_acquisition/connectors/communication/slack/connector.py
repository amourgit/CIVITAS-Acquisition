"""
SlackConnector — connecteur Slack complet.

Ressources acquises :
  - messages      : messages de canaux avec threads et réactions
  - files         : fichiers partagés dans les canaux
  - channels      : liste et métadonnées des canaux
  - users         : répertoire des membres

Canaux d'entrée :
  - POLLING  : pull périodique via conversations.history
  - WEBHOOK  : Events API Slack (socket mode ou HTTP)
  - MANUAL   : pull one-shot

Auth :
  - Bot Token (xoxb-...) : recommandé, scopes granulaires
  - User Token (xoxp-...) : accès utilisateur complet

OAuth scopes requis (bot) :
  channels:history, channels:read, groups:history, groups:read,
  im:history, im:read, mpim:history, mpim:read,
  files:read, users:read, users:read.email, reactions:read

Cursor : dernier timestamp Unix (ts) de chaque canal.

Config options :
  channel_ids    : list[str]   — canaux spécifiques (ex: ["C01234", "C05678"])
  channel_types  : list[str]   — ["public_channel","private_channel","im","mpim"]
  resource_types : list[str]   — ["messages","files","channels","users"]
  include_bots   : bool        — inclure les messages de bots (défaut: False)
  include_threads: bool        — récupérer les threads complets (défaut: True)
  oldest         : str         — timestamp Unix de départ (curseur initial)
  max_messages   : int         — max messages par canal par cycle (défaut: 1000)
"""
from __future__ import annotations

import json
import logging
import mimetypes
import time
from typing import Any, AsyncIterator, Optional

import aiohttp

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
    ConnectorAuthenticationError, ConnectorRateLimitError,
    ConnectorTemporaryError, ConnectorFatalError,
)

logger = logging.getLogger(__name__)

SLACK_API = "https://slack.com/api"


class SlackConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="slack",
            display_name="Slack",
            version="1.0.0",
            source_category=SourceCategory.COMMUNICATION,
            supported_channels=frozenset([
                ChannelType.POLLING, ChannelType.WEBHOOK, ChannelType.MANUAL,
            ]),
            supported_mime_types=frozenset([
                "application/json", "text/plain", "*/*",
            ]),
            required_credentials=(
                CredentialSpec(
                    key="bot_token",
                    description="Slack Bot Token (xoxb-...)",
                    sensitive=True,
                ),
            ),
            optional_credentials=(
                CredentialSpec(
                    key="signing_secret",
                    description="Slack Signing Secret pour vérification webhooks",
                    required=False, sensitive=True,
                ),
            ),
            rate_limit=RateLimit(requests_per_second=1.0, burst_size=3),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    # ── Connect ───────────────────────────────────────────────────────────────

    async def _do_connect(self, config: ConnectorConfig) -> None:
        self._token  = config.get_credential("bot_token")
        self._secret = config.credentials.get("signing_secret")
        timeout = aiohttp.ClientTimeout(total=config.get_option("timeout_s", 30.0))
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type":  "application/json; charset=utf-8",
            },
        )
        # Options
        self._channel_ids    = config.get_option("channel_ids", [])
        self._channel_types  = config.get_option("channel_types", ["public_channel"])
        self._resource_types = config.get_option("resource_types", ["messages"])
        self._include_bots   = config.get_option("include_bots", False)
        self._include_threads = config.get_option("include_threads", True)
        self._max_messages   = config.get_option("max_messages", 1000)

        # Vérifier auth
        data = await self._api("auth.test")
        if not data.get("ok"):
            raise ConnectorAuthenticationError(
                "slack", data.get("error", "auth.test failed")
            )
        self._bot_user_id = data.get("user_id", "")
        logger.info("Slack connected as %s (%s)", data.get("user"), data.get("team"))

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_session") and not self._session.closed:
            await self._session.close()

    # ── Health ────────────────────────────────────────────────────────────────

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            data = await self._api("auth.test")
            ok = data.get("ok", False)
            return HealthStatus.ok(
                latency_ms=(time.monotonic() - start) * 1000,
                team=data.get("team", ""),
                bot_user=data.get("user", ""),
            ) if ok else HealthStatus.fail(data.get("error", "unknown"))
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def discover(self) -> DiscoveryResult:
        channels = await self._list_all_channels()
        resources = tuple(
            f"slack://channel/{c['id']}/{c.get('name', c['id'])}"
            for c in channels
        )
        return DiscoveryResult(resources=resources, total=len(resources))

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors = json.loads(cursor.value) if cursor else {}
        updated = dict(cursors)
        count   = 0

        # Résoudre les canaux
        channels = self._channel_ids
        if not channels:
            all_channels = await self._list_all_channels()
            channels = [c["id"] for c in all_channels]

        # ── Messages ──────────────────────────────────────────────────────────
        if "messages" in self._resource_types:
            for channel_id in channels:
                if count >= batch_size:
                    break
                oldest = cursors.get(f"msg:{channel_id}", "0")
                async for doc in self._fetch_messages(
                    channel_id, oldest=oldest,
                    batch_size=min(batch_size - count, self._max_messages),
                ):
                    if count >= batch_size:
                        break
                    updated[f"msg:{channel_id}"] = doc.source_metadata.get("ts", oldest)
                    yield self._stamp(doc, updated)
                    count += 1

        # ── Files ─────────────────────────────────────────────────────────────
        if "files" in self._resource_types:
            since_ts = int(float(min(cursors.values(), default="0"))) if cursors else 0
            async for doc in self._fetch_files(
                since_ts=since_ts,
                batch_size=min(batch_size - count, 100),
            ):
                if count >= batch_size:
                    break
                yield self._stamp(doc, updated)
                count += 1

        # ── Channels metadata ─────────────────────────────────────────────────
        if "channels" in self._resource_types and count < batch_size:
            all_channels = await self._list_all_channels()
            for ch in all_channels:
                if count >= batch_size:
                    break
                content = json.dumps(ch, ensure_ascii=False).encode()
                yield RawDocument.create(
                    instance_id=self.instance_id, connector_id="slack",
                    uri=f"slack://channel/{ch['id']}",
                    content=content, content_type="application/json",
                    cursor=self._make_cursor(updated),
                    tags=("channel",),
                    source_metadata={"resource_type": "channel", **ch},
                )
                count += 1

        # ── Users ─────────────────────────────────────────────────────────────
        if "users" in self._resource_types and count < batch_size:
            async for doc in self._fetch_users(batch_size=batch_size - count):
                if count >= batch_size:
                    break
                yield self._stamp(doc, updated)
                count += 1

        logger.info("Slack pull completed: %d documents", count)

    # ── Messages ──────────────────────────────────────────────────────────────

    async def _fetch_messages(
        self,
        channel_id: str,
        oldest: str = "0",
        batch_size: int = 200,
    ) -> AsyncIterator[RawDocument]:
        params: dict[str, Any] = {
            "channel": channel_id,
            "limit": min(batch_size, 200),
            "oldest": oldest,
        }
        cursor_token: Optional[str] = None
        fetched = 0

        while fetched < batch_size:
            if cursor_token:
                params["cursor"] = cursor_token
            data = await self._api("conversations.history", params=params)
            messages = data.get("messages", [])

            for msg in messages:
                if fetched >= batch_size:
                    break
                # Filtres
                if not self._include_bots and msg.get("bot_id"):
                    continue
                if msg.get("subtype") in ("channel_join", "channel_leave", "bot_message"):
                    if not self._include_bots:
                        continue

                ts  = msg.get("ts", "")
                doc = self._map_message(msg, channel_id, ts)

                # Threads
                if self._include_threads and msg.get("thread_ts") == ts and msg.get("reply_count", 0) > 0:
                    replies = await self._fetch_thread_replies(channel_id, ts)
                    if replies:
                        import dataclasses
                        enriched = json.loads(doc.content)
                        enriched["replies"] = replies
                        doc = dataclasses.replace(
                            doc,
                            content=json.dumps(enriched, ensure_ascii=False).encode(),
                        )
                yield doc
                fetched += 1

            meta = data.get("response_metadata", {})
            cursor_token = meta.get("next_cursor")
            if not cursor_token:
                break

    async def _fetch_thread_replies(self, channel_id: str, thread_ts: str) -> list[dict]:
        data = await self._api("conversations.replies", params={
            "channel": channel_id, "ts": thread_ts, "limit": 200,
        })
        msgs = data.get("messages", [])[1:]   # skip parent
        return [
            {"user": m.get("user", ""), "text": m.get("text", ""), "ts": m.get("ts", "")}
            for m in msgs
        ]

    def _map_message(self, msg: dict, channel_id: str, ts: str) -> RawDocument:
        payload = {
            "ts":         ts,
            "channel_id": channel_id,
            "user":       msg.get("user", msg.get("bot_id", "")),
            "text":       msg.get("text", ""),
            "type":       msg.get("type", "message"),
            "subtype":    msg.get("subtype"),
            "reactions":  msg.get("reactions", []),
            "attachments": msg.get("attachments", []),
            "blocks":     msg.get("blocks", []),
            "thread_ts":  msg.get("thread_ts"),
            "reply_count": msg.get("reply_count", 0),
        }
        content = json.dumps(payload, ensure_ascii=False).encode()
        uri = f"slack://message/{channel_id}/{ts}"
        return RawDocument.create(
            instance_id=self.instance_id, connector_id="slack",
            uri=uri, content=content, content_type="application/json",
            version=ts,
            cursor=Cursor(value=ts, source_type="timestamp",
                          connector_id="slack", instance_id=self.instance_id),
            tags=("message", f"channel:{channel_id}"),
            source_metadata={
                "resource_type": "message",
                "channel_id": channel_id,
                "ts": ts,
                "user": payload["user"],
                "has_thread": payload["reply_count"] > 0,
            },
        )

    # ── Files ─────────────────────────────────────────────────────────────────

    async def _fetch_files(
        self, since_ts: int = 0, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        params: dict = {"count": min(batch_size, 100)}
        if since_ts:
            params["ts_from"] = since_ts
        cursor_page = 1

        while True:
            params["page"] = cursor_page
            data = await self._api("files.list", params=params)
            files = data.get("files", [])
            if not files:
                break

            for f in files:
                if f.get("mode") == "tombstone":
                    continue
                content = await self._download_file(f)
                if content is None:
                    continue
                ext  = "." + f["name"].rsplit(".", 1)[-1].lower() if "." in f["name"] else ""
                mime = f.get("mimetype") or (mimetypes.guess_type(f["name"])[0] or "application/octet-stream")
                yield RawDocument.create(
                    instance_id=self.instance_id, connector_id="slack",
                    uri=f"slack://file/{f['id']}/{f['name']}",
                    content=content, content_type=mime,
                    version=str(f.get("timestamp", "")),
                    cursor=Cursor(
                        value=str(f.get("timestamp", "")),
                        source_type="timestamp",
                        connector_id="slack", instance_id=self.instance_id,
                    ),
                    tags=("file", f"type:{f.get('filetype', 'unknown')}"),
                    source_metadata={
                        "resource_type": "file",
                        "file_id": f["id"],
                        "name": f["name"],
                        "size": f.get("size", 0),
                        "filetype": f.get("filetype"),
                        "user": f.get("user", ""),
                        "timestamp": f.get("timestamp"),
                        "channels": f.get("channels", []),
                        "url_private": f.get("url_private", ""),
                    },
                )

            paging = data.get("paging", {})
            if cursor_page >= paging.get("pages", 1):
                break
            cursor_page += 1

    async def _download_file(self, file_info: dict) -> Optional[bytes]:
        url = file_info.get("url_private_download") or file_info.get("url_private")
        if not url:
            return None
        try:
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    return await resp.read()
        except Exception as exc:
            logger.debug("File download failed %s: %s", file_info.get("id"), exc)
        return None

    # ── Users ─────────────────────────────────────────────────────────────────

    async def _fetch_users(self, batch_size: int = 200) -> AsyncIterator[RawDocument]:
        cursor_token: Optional[str] = None
        fetched = 0
        while fetched < batch_size:
            params: dict = {"limit": min(200, batch_size - fetched)}
            if cursor_token:
                params["cursor"] = cursor_token
            data = await self._api("users.list", params=params)
            for member in data.get("members", []):
                if member.get("deleted") or member.get("is_bot"):
                    continue
                content = json.dumps(member, ensure_ascii=False).encode()
                yield RawDocument.create(
                    instance_id=self.instance_id, connector_id="slack",
                    uri=f"slack://user/{member['id']}",
                    content=content, content_type="application/json",
                    version=str(member.get("updated", "")),
                    tags=("user",),
                    source_metadata={
                        "resource_type": "user",
                        "user_id": member["id"],
                        "name": member.get("name", ""),
                        "real_name": member.get("real_name", ""),
                        "email": member.get("profile", {}).get("email", ""),
                    },
                )
                fetched += 1
                if fetched >= batch_size:
                    break
            meta = data.get("response_metadata", {})
            cursor_token = meta.get("next_cursor")
            if not cursor_token:
                break

    # ── Channels list ─────────────────────────────────────────────────────────

    async def _list_all_channels(self) -> list[dict]:
        channels: list[dict] = []
        cursor_token: Optional[str] = None
        while True:
            params: dict = {
                "types": ",".join(self._channel_types),
                "exclude_archived": True,
                "limit": 200,
            }
            if cursor_token:
                params["cursor"] = cursor_token
            data = await self._api("conversations.list", params=params)
            channels.extend(data.get("channels", []))
            meta = data.get("response_metadata", {})
            cursor_token = meta.get("next_cursor")
            if not cursor_token:
                break
        return channels

    # ── Webhook verification (Events API) ─────────────────────────────────────

    def verify_webhook(self, body: bytes, headers: dict[str, str]) -> bool:
        """
        Vérifie la signature d'un événement Slack Events API.
        X-Slack-Signature: v0=<hmac_sha256>
        X-Slack-Request-Timestamp: <unix_ts>
        """
        if not self._secret:
            logger.warning("No signing secret configured — skipping verification")
            return True
        import hashlib
        import hmac as hmaclib
        normalized = {k.lower(): v for k, v in headers.items()}
        ts  = normalized.get("x-slack-request-timestamp", "")
        sig = normalized.get("x-slack-signature", "")
        if not ts or not sig:
            return False
        # Reject si trop vieux (> 5 minutes)
        if abs(time.time() - float(ts)) > 300:
            return False
        basestring = f"v0:{ts}:{body.decode('utf-8')}".encode()
        computed = "v0=" + hmaclib.new(
            self._secret.encode(), basestring, hashlib.sha256
        ).hexdigest()
        return hmaclib.compare_digest(computed, sig)

    # ── Slack API client ──────────────────────────────────────────────────────

    async def _api(self, method: str, params: dict | None = None) -> dict[str, Any]:
        url = f"{SLACK_API}/{method}"
        assert self._session
        async with self._session.get(url, params=params) as resp:
            if resp.status == 429:
                retry = float(resp.headers.get("Retry-After", 60))
                raise ConnectorRateLimitError("slack", retry_after_s=retry)
            if resp.status in (500, 502, 503):
                raise ConnectorTemporaryError(f"Slack {resp.status}")
            data = await resp.json()
            if not data.get("ok"):
                err = data.get("error", "unknown")
                if err in ("invalid_auth", "token_revoked", "not_authed"):
                    raise ConnectorAuthenticationError("slack", err)
                logger.debug("Slack API warning: %s → %s", method, err)
            return data

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _make_cursor(self, cursors: dict) -> Cursor:
        return Cursor(
            value=json.dumps(cursors, sort_keys=True),
            source_type="token",
            connector_id="slack",
            instance_id=self.instance_id,
        )

    def _stamp(self, doc: RawDocument, cursors: dict) -> RawDocument:
        import dataclasses
        return dataclasses.replace(doc, cursor=self._make_cursor(cursors))
