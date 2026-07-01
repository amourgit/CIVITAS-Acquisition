"""
WorkerDispatcher — distribue les tâches d'acquisition aux workers.

Les Channels publient des WorkerTasks ici.
Le Dispatcher gère la file d'attente, la priorité et le backpressure.
Les Workers consomment depuis le pool.

Découplage fondamental :
  Channel → Dispatcher (publier) ← → Worker Pool (consommer)

Sans ce découplage, un webhook attendrait la fin du pull() avant de
retourner une réponse HTTP. Inacceptable.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from civitas_acquisition.contracts.ports.worker_port import (
    DispatcherPort,
    WorkerTask,
    TaskPriority,
    TaskStatus,
)
from civitas_acquisition.policies.backpressure import BackpressurePolicy, BackpressureError

logger = logging.getLogger(__name__)


class WorkerDispatcher(DispatcherPort):
    """
    Dispatcher de tâches avec priorité et backpressure.

    Usage :
        dispatcher = WorkerDispatcher(pool, policy=WEBHOOK_BACKPRESSURE)
        await dispatcher.dispatch(task)   # Non-bloquant (sauf BLOCK strategy)
    """

    def __init__(
        self,
        pool: "WorkerPool",  # type: ignore[name-defined]  # noqa: F821
        policy: BackpressurePolicy | None = None,
    ) -> None:
        from civitas_acquisition.policies.backpressure import POLLING_BACKPRESSURE
        self._pool = pool
        self._policy = policy or POLLING_BACKPRESSURE
        # Priority queue : (priority_value, task) où priority_value bas = haute priorité
        self._queue: asyncio.PriorityQueue[tuple[int, WorkerTask]] = asyncio.PriorityQueue(
            maxsize=self._policy.max_queue_size
        )
        self._active_task_ids: set[str] = set()

    async def dispatch(self, task: WorkerTask) -> None:
        """
        Enfile la tâche pour exécution.
        Applique la politique de backpressure si la file est pleine.
        """
        current_size = self._queue.qsize()

        if self._policy.is_high_watermark(current_size):
            logger.warning(
                "Queue high watermark reached: %d/%d tasks",
                current_size, self._policy.max_queue_size,
            )

        if self._policy.is_full(current_size):
            await self._handle_backpressure(task, current_size)
            return

        priority_value = self._priority_value(task.priority)
        self._active_task_ids.add(task.task_id)
        await self._queue.put((priority_value, task))

        logger.debug(
            "Task dispatched: %s (priority=%s, queue_size=%d)",
            task.task_id[:8], task.priority.name, self._queue.qsize(),
        )

        # Notifier le pool qu'une tâche est disponible
        await self._pool.notify()

    async def cancel(self, task_id: str) -> None:
        """
        Annule une tâche. Si en file : marquée annulée.
        Si en cours d'exécution : délégué au worker actif.
        """
        self._active_task_ids.discard(task_id)
        # L'annulation en cours d'exécution est gérée par le WorkerPool

    async def queue_size(self) -> int:
        return self._queue.qsize()

    async def active_count(self) -> int:
        return await self._pool.active_count()

    async def get_next(self) -> Optional[WorkerTask]:
        """Consommé par le WorkerPool pour récupérer la prochaine tâche."""
        try:
            _, task = self._queue.get_nowait()
            if task.task_id not in self._active_task_ids:
                task.status = TaskStatus.CANCELLED
            return task
        except asyncio.QueueEmpty:
            return None

    def _priority_value(self, priority: TaskPriority) -> int:
        return {
            TaskPriority.URGENT: 0,
            TaskPriority.HIGH:   1,
            TaskPriority.NORMAL: 2,
            TaskPriority.LOW:    3,
        }[priority]

    async def _handle_backpressure(self, task: WorkerTask, current_size: int) -> None:
        from civitas_acquisition.policies.backpressure import BackpressureStrategy
        strategy = self._policy.strategy
        max_size = self._policy.max_queue_size

        if strategy == BackpressureStrategy.REJECT:
            raise BackpressureError(current_size, max_size)

        elif strategy == BackpressureStrategy.DROP:
            logger.warning(
                "Backpressure DROP: task %s discarded (queue=%d/%d)",
                task.task_id[:8], current_size, max_size,
            )
            task.status = TaskStatus.CANCELLED

        elif strategy == BackpressureStrategy.BLOCK:
            logger.info(
                "Backpressure BLOCK: waiting for queue space (timeout=%.1fs)",
                self._policy.max_wait_s,
            )
            try:
                priority_value = self._priority_value(task.priority)
                await asyncio.wait_for(
                    self._queue.put((priority_value, task)),
                    timeout=self._policy.max_wait_s,
                )
                self._active_task_ids.add(task.task_id)
            except asyncio.TimeoutError:
                raise BackpressureError(current_size, max_size)
