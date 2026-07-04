"""
GmailConnector — acquisition d'emails via Gmail API v1.

Ressources acquises :
  - emails     : messages complets (headers + body texte/html)
  - attachments: pièces jointes
  - threads    : fils de discussion complets
  - labels     : structure des labels

Auth : OAuth2 via service account ou user credentials (google-auth)
Scopes requis : gmail.readonly

Config options :
  resource_types  : list["emails","threads","labels"]
  label_ids       : list[str]  — labels à surveiller (défaut: ["INBOX"])
  max_results     : int        — messages par cycle (défaut: 500)
  include_spam    : bool       — inclure SPAM/TRASH (défaut: False)
  after_date      : str        — date ISO-8601 de départ
  query           : str        — filtre Gmail (ex: "from:boss@company.com")
  include_body    : bool       — extraire le body (défaut: True)
  body_format     : str        — "text" | "html" | "both" (défaut: "text")
  max_attachment_size: int     — taille max pièces jointes en bytes (défaut: 5MB)

Credentials :
  service_account_json : JSON de service account (recommandé)
  ou
  access_token + refresh_token + client_id + client_secret (OAuth2 user flow)
"""
from __future__ import annotations

import base64
import json
import logging
import re
import time
from email import message_from_bytes
from email.utils import parsedate_to_datetime
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
    ConnectorAuthenticationError, ConnectorRateLimitError, ConnectorTemporaryError,
)

logger = logging.getLogger(__name__)

GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"
MAX_ATTACHMENT_SIZE = 5 * 1024 * 1024  # 5MB


class GmailConnector(BaseConnector):

    def manifest(self) -> ConnectorManifest:
        return ConnectorManifest(
            connector_id="gmail",
            display_name="Gmail",
            version="1.0.0",
            source_category=SourceCategory.COMMUNICATION,
            supported_channels=frozenset([ChannelType.POLLING, ChannelType.MANUAL]),
            supported_mime_types=frozenset(["application/json", "text/plain", "text/html"]),
            required_credentials=(
                CredentialSpec(key="access_token", description="OAuth2 Access Token Gmail", sensitive=True),
            ),
            optional_credentials=(
                CredentialSpec(key="refresh_token",    description="OAuth2 Refresh Token",  required=False, sensitive=True),
                CredentialSpec(key="client_id",        description="OAuth2 Client ID",      required=False, sensitive=False),
                CredentialSpec(key="client_secret",    description="OAuth2 Client Secret",  required=False, sensitive=True),
                CredentialSpec(key="service_account_json", description="Service Account JSON", required=False, sensitive=True),
            ),
            rate_limit=RateLimit(requests_per_second=5.0, burst_size=20),
            supports_cursor=True,
            supports_delta=True,
            supports_discovery=True,
        )

    async def _do_connect(self, config: ConnectorConfig) -> None:
        self._access_token   = config.get_credential("access_token")
        self._refresh_token  = config.credentials.get("refresh_token")
        self._client_id      = config.credentials.get("client_id")
        self._client_secret  = config.credentials.get("client_secret")

        timeout = aiohttp.ClientTimeout(total=config.get_option("timeout_s", 30.0))
        self._session = aiohttp.ClientSession(timeout=timeout)

        self._label_ids       = config.get_option("label_ids", ["INBOX"])
        self._resource_types  = config.get_option("resource_types", ["emails"])
        self._max_results     = config.get_option("max_results", 500)
        self._include_spam    = config.get_option("include_spam", False)
        self._include_body    = config.get_option("include_body", True)
        self._body_format     = config.get_option("body_format", "text")
        self._query           = config.get_option("query", "")
        self._max_attach_size = config.get_option("max_attachment_size", MAX_ATTACHMENT_SIZE)

        # Vérifier auth
        profile = await self._api("GET", "/profile")
        if "error" in profile:
            raise ConnectorAuthenticationError("gmail", profile.get("error", {}).get("message", "auth failed"))
        self._email_address = profile.get("emailAddress", "")
        logger.info("Gmail connected: %s", self._email_address)

    async def _do_disconnect(self) -> None:
        if hasattr(self, "_session") and not self._session.closed:
            await self._session.close()

    async def healthcheck(self) -> HealthStatus:
        start = time.monotonic()
        try:
            profile = await self._api("GET", "/profile")
            if "error" in profile:
                return HealthStatus.fail(str(profile.get("error")))
            return HealthStatus.ok(
                latency_ms=(time.monotonic() - start) * 1000,
                email=profile.get("emailAddress"),
                total_messages=profile.get("messagesTotal"),
            )
        except Exception as exc:
            return HealthStatus.fail(str(exc))

    async def discover(self) -> DiscoveryResult:
        """Liste tous les labels disponibles."""
        data = await self._api("GET", "/labels")
        labels = data.get("labels", [])
        resources = tuple(
            f"gmail://label/{lb['id']}/{lb.get('name', lb['id'])}"
            for lb in labels
        )
        return DiscoveryResult(resources=resources, total=len(resources))

    async def _do_pull(
        self, cursor: Optional[Cursor] = None, batch_size: int = 100,
    ) -> AsyncIterator[RawDocument]:
        cursors = json.loads(cursor.value) if cursor else {}
        updated = dict(cursors)
        count   = 0

        if "emails" in self._resource_types or "threads" in self._resource_types:
            async for doc in self._fetch_messages(cursors, batch_size):
                if count >= batch_size:
                    break
                updated[f"msg:{self._email_address}"] = doc.source_metadata.get("internal_date", "")
                yield self._stamp(doc, updated)
                count += 1

        if "labels" in self._resource_types and count < batch_size:
            data = await self._api("GET", "/labels")
            for lb in data.get("labels", []):
                if count >= batch_size:
                    break
                content = json.dumps(lb, ensure_ascii=False).encode()
                yield RawDocument.create(
                    instance_id=self.instance_id, connector_id="gmail",
                    uri=f"gmail://label/{lb['id']}",
                    content=content, content_type="application/json",
                    tags=("label",),
                    source_metadata={"resource_type": "label", **lb},
                )
                count += 1

    async def _fetch_messages(
        self, cursors: dict, batch_size: int,
    ) -> AsyncIterator[RawDocument]:
        page_token: Optional[str] = None
        since = cursors.get(f"msg:{getattr(self, '_email_address', '')}")
        count = 0

        while count < batch_size:
            params: dict[str, Any] = {
                "maxResults": min(self._max_results, batch_size - count),
                "labelIds": self._label_ids,
            }
            query_parts = []
            if self._query:
                query_parts.append(self._query)
            if since:
                query_parts.append(f"after:{since}")
            if not self._include_spam:
                params["labelIds"] = [lb for lb in self._label_ids if lb not in ("SPAM", "TRASH")]
            if query_parts:
                params["q"] = " ".join(query_parts)
            if page_token:
                params["pageToken"] = page_token

            data = await self._api("GET", "/messages", params=params)
            messages = data.get("messages", [])
            if not messages:
                break

            for msg_ref in messages:
                if count >= batch_size:
                    return
                msg_id = msg_ref["id"]
                try:
                    msg_data = await self._api("GET", f"/messages/{msg_id}",
                                               params={"format": "full"})
                    doc = self._map_message(msg_data)
                    yield doc
                    count += 1
                except Exception as exc:
                    logger.warning("Error fetching message %s: %s", msg_id, exc)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

    def _map_message(self, msg: dict[str, Any]) -> RawDocument:
        headers  = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        subject  = headers.get("subject", "(no subject)")
        from_    = headers.get("from", "")
        to_      = headers.get("to", "")
        date_str = headers.get("date", "")
        msg_id   = msg.get("id", "")
        thread_id = msg.get("threadId", "")
        snippet  = msg.get("snippet", "")
        labels   = msg.get("labelIds", [])
        internal = msg.get("internalDate", "")

        body_text, body_html, attachments = self._extract_payload(msg.get("payload", {}))

        payload = {
            "id": msg_id,
            "thread_id": thread_id,
            "subject": subject,
            "from": from_,
            "to": to_,
            "date": date_str,
            "snippet": snippet,
            "labels": labels,
            "body_text": body_text if self._body_format in ("text", "both") else "",
            "body_html": body_html if self._body_format in ("html", "both") else "",
            "attachments": [{"name": a["name"], "size": a["size"], "mime": a["mime"]} for a in attachments],
            "internal_date": internal,
        }
        content = json.dumps(payload, ensure_ascii=False).encode()

        return RawDocument.create(
            instance_id=self.instance_id, connector_id="gmail",
            uri=f"gmail://message/{msg_id}",
            content=content, content_type="application/json",
            version=internal,
            cursor=Cursor(value=internal, source_type="sequence",
                          connector_id="gmail", instance_id=self.instance_id),
            tags=("email", f"thread:{thread_id}"),
            source_metadata={
                "resource_type": "email",
                "msg_id": msg_id,
                "thread_id": thread_id,
                "subject": subject,
                "from": from_,
                "to": to_,
                "labels": labels,
                "internal_date": internal,
                "has_attachments": len(attachments) > 0,
            },
        )

    def _extract_payload(self, payload: dict) -> tuple[str, str, list[dict]]:
        """Extrait body text, html et attachments depuis le payload MIME."""
        body_text = ""
        body_html = ""
        attachments: list[dict] = []

        def _process_part(part: dict) -> None:
            nonlocal body_text, body_html
            mime = part.get("mimeType", "")
            body = part.get("body", {})
            filename = part.get("filename", "")

            if filename and body.get("size", 0) <= self._max_attach_size:
                attachments.append({
                    "name": filename, "mime": mime,
                    "size": body.get("size", 0),
                    "attachment_id": body.get("attachmentId", ""),
                })
                return

            if mime == "text/plain" and not body_text:
                data = body.get("data", "")
                if data:
                    body_text = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
            elif mime == "text/html" and not body_html:
                data = body.get("data", "")
                if data:
                    body_html = base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")

            for sub in part.get("parts", []):
                _process_part(sub)

        _process_part(payload)
        return body_text, body_html, attachments

    async def _api(
        self, method: str, path: str, params: dict | None = None, body: dict | None = None,
    ) -> dict[str, Any]:
        url = f"{GMAIL_API}{path}"
        headers = {"Authorization": f"Bearer {self._access_token}"}
        async with self._session.request(method, url, params=params, json=body, headers=headers) as resp:
            if resp.status == 401:
                await self._refresh_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                async with self._session.request(method, url, params=params, json=body, headers=headers) as r2:
                    return await r2.json()
            if resp.status == 429:
                raise ConnectorRateLimitError("gmail", retry_after_s=float(resp.headers.get("Retry-After", 60)))
            if resp.status in (500, 502, 503):
                raise ConnectorTemporaryError(f"Gmail API {resp.status}")
            return await resp.json()

    async def _refresh_access_token(self) -> None:
        if not self._refresh_token or not self._client_id:
            raise ConnectorAuthenticationError("gmail", "Cannot refresh — no refresh_token or client_id")
        async with self._session.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret or "",
            },
        ) as resp:
            data = await resp.json()
            if "access_token" not in data:
                raise ConnectorAuthenticationError("gmail", f"Token refresh failed: {data.get('error', 'unknown')}")
            self._access_token = data["access_token"]
            logger.info("Gmail access token refreshed")

    def _stamp(self, doc: RawDocument, cursors: dict) -> RawDocument:
        import dataclasses
        return dataclasses.replace(
            doc,
            cursor=Cursor(value=json.dumps(cursors, sort_keys=True), source_type="token",
                          connector_id="gmail", instance_id=self.instance_id),
        )
