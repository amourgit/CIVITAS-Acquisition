"""
PollingChannel — canal d'acquisition par polling périodique.

Responsabilité : boucle de scheduling autour d'un ConnectorPort.
  1. Charge le curseur courant depuis le tracker
  2. Pull les documents depuis le connecteur
  3. Les soumet au processor
  4. Adapte l'intervalle selon les résultats (via la stratégie)

Ne connaît pas la logique métier. Que de l'orchestration de boucle.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from civitas_acquisition.contracts.ports.connector_port import ConnectorPort
from civitas_acquisition.contracts.ports.channel_port import ChannelPort
from civitas_acquisition.contracts.ports.scheduler_port import JobResult
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.acquisition_job import JobTrigger
from civitas_acquisition.contracts.models.connector_manifest import ChannelType
from civitas_acquisition.domain.acquisition_job import AcquisitionJob
from civitas_acquisition.processing.acquisition_processor import AcquisitionProcessor
from civitas_acquisition.execution.strategies.polling_strategy import AdaptivePollingStrategy

logger = logging.getLogger(__name__)


class PollingChannel(ChannelPort):
    """
    Canal de polling périodique pour un connecteur.

    Un PollingChannel = une instance connecteur + une stratégie de scheduling.
    Plusieurs PollingChannels peuvent tourner en parallèle pour différentes instances.
    """

    def __init__(
        self,
        connector: ConnectorPort,
        processor: AcquisitionProcessor,
        strategy: Optional[AdaptivePollingStrategy] = None,
        batch_size: int = 100,
    ) -> None:
        self._connector = connector
        self._processor = processor
        self._strategy = strategy or AdaptivePollingStrategy()
        self._batch_size = batch_size
        self._running = False
        self._current_job: Optional[AcquisitionJob] = None
        self._last_cursor: Optional[Cursor] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        manifest = self._connector.manifest()
        logger.info("PollingChannel started for '%s'", manifest.connector_id)

        async for result in self._strategy.schedule(self._pull_cycle):
            if not self._running:
                break
            if result.has_work:
                self._strategy.reset()
            else:
                self._strategy.backoff()

    async def stop(self) -> None:
        self._running = False
        logger.info(
            "PollingChannel stopping for '%s'",
            self._connector.manifest().connector_id,
        )

    def is_running(self) -> bool:
        return self._running

    async def _pull_cycle(self) -> JobResult:
        """Un cycle complet : pull + process + accumulation résultats."""
        manifest = self._connector.manifest()
        config = getattr(self._connector, "config", None)
        instance_id = config.instance_id if config else "unknown"

        job = AcquisitionJob.create(
            connector_id=manifest.connector_id,
            instance_id=instance_id,
            channel_type=ChannelType.POLLING,
            trigger=JobTrigger.SCHEDULED,
            config=config,
            starting_cursor=self._last_cursor,
        )
        job.start()
        self._current_job = job

        try:
            async for doc in self._connector.pull(
                cursor=self._last_cursor,
                batch_size=self._batch_size,
            ):
                result = await self._processor.process(doc, job=job)
                if doc.cursor and result.success and result.status == "done":
                    self._last_cursor = doc.cursor

            job.complete()

        except Exception as exc:
            logger.error(
                "PollingChannel error in '%s': %s",
                manifest.connector_id, exc, exc_info=True,
            )
            job.fail(str(exc))
        finally:
            self._current_job = None

        record = job.to_record()
        return JobResult(
            documents_found=record.total_documents,
            documents_processed=record.documents_acquired,
            documents_skipped=record.documents_skipped,
            documents_failed=record.documents_failed,
        )
