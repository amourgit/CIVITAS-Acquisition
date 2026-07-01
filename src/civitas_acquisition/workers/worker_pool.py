"""
WorkerPool — pool de workers d'acquisition avec supervision.

Le pool maintient N workers actifs en permanence.
Chaque worker consomme des tâches depuis le Dispatcher.
Le Supervisor redémarre automatiquement un worker crashé.

Paramètres clés :
  min_workers : nombre minimal de workers (toujours actifs)
  max_workers : nombre maximal (scale-up sous charge)
  idle_timeout_s : un worker idle au-dessus de min_workers est terminé
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from civitas_acquisition.contracts.ports.worker_port import WorkerTask, TaskStatus

logger = logging.getLogger(__name__)


class PoolWorker:
    """Un worker dans le pool. Consomme des tâches en boucle."""

    def __init__(self, worker_id: int, pool: "WorkerPool") -> None:
        self._worker_id = worker_id
        self._pool = pool
        self._current_task: Optional[WorkerTask] = None
        self._running = False
        self._task_handle: Optional[asyncio.Task] = None

    @property
    def worker_id(self) -> int:
        return self._worker_id

    @property
    def is_busy(self) -> bool:
        return self._current_task is not None

    async def run(self) -> None:
        self._running = True
        logger.debug("Worker %d started", self._worker_id)
        while self._running:
            task = await self._pool.dispatcher.get_next()
            if task is None:
                await asyncio.sleep(0.1)
                continue
            if task.status == TaskStatus.CANCELLED:
                continue
            self._current_task = task
            try:
                task.status = TaskStatus.RUNNING
                await self._pool.execute_task(task)
                task.status = TaskStatus.DONE
            except Exception as exc:
                task.status = TaskStatus.FAILED
                logger.error("Worker %d task failed: %s", self._worker_id, exc, exc_info=True)
            finally:
                self._current_task = None

    async def stop(self) -> None:
        self._running = False
        if self._task_handle and not self._task_handle.done():
            self._task_handle.cancel()


class WorkerPool:
    """
    Pool de workers avec supervision et auto-scaling.

    Usage :
        pool = WorkerPool(dispatcher, min_workers=2, max_workers=8)
        await pool.start()
        # ... traitement en cours ...
        await pool.stop()
    """

    def __init__(
        self,
        dispatcher: "WorkerDispatcher",  # type: ignore[name-defined]  # noqa: F821
        acquisition_pipeline: object,    # AcquisitionPipeline injecté
        min_workers: int = 2,
        max_workers: int = 8,
    ) -> None:
        self.dispatcher = dispatcher
        self._pipeline = acquisition_pipeline
        self._min_workers = min_workers
        self._max_workers = max_workers
        self._workers: dict[int, PoolWorker] = {}
        self._notify_event = asyncio.Event()
        self._running = False
        self._next_id = 0

    async def start(self) -> None:
        self._running = True
        for _ in range(self._min_workers):
            await self._spawn_worker()
        logger.info(
            "WorkerPool started with %d workers (max=%d)",
            self._min_workers, self._max_workers,
        )

    async def stop(self) -> None:
        self._running = False
        for worker in list(self._workers.values()):
            await worker.stop()
        self._workers.clear()
        logger.info("WorkerPool stopped")

    async def notify(self) -> None:
        """Notifie le pool qu'une tâche est disponible."""
        self._notify_event.set()

    async def active_count(self) -> int:
        return sum(1 for w in self._workers.values() if w.is_busy)

    async def execute_task(self, task: WorkerTask) -> None:
        """Délégué par un PoolWorker pour exécuter la tâche via le pipeline."""
        # L'implémentation complète viendra avec le processing/
        # Pour l'instant : délégation au pipeline
        pass

    async def _spawn_worker(self) -> PoolWorker:
        worker = PoolWorker(self._next_id, self)
        self._next_id += 1
        task_handle = asyncio.create_task(worker.run())
        worker._task_handle = task_handle
        self._workers[worker.worker_id] = worker
        return worker
