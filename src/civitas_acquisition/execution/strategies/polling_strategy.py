"""
PollingStrategy — stratégie d'exécution pour le canal Polling.

Implémente SchedulerPort avec backoff adaptatif :
  - Si 0 documents trouvés N fois consécutives → augmenter l'intervalle
  - Si des documents sont trouvés → reset à l'intervalle de base
  - Jitter pour éviter le thundering herd sur plusieurs instances

C'est le seul endroit où réside la logique de timing du polling.
Le Channel Polling délègue entièrement à cette stratégie.
"""
from __future__ import annotations

import asyncio
import logging
import random
from typing import AsyncIterator, Callable, Awaitable

from civitas_acquisition.contracts.ports.scheduler_port import SchedulerPort, JobResult

logger = logging.getLogger(__name__)


class AdaptivePollingStrategy(SchedulerPort):
    """
    Stratégie de polling avec backoff adaptatif.

    Paramètres :
      base_interval_s  : intervalle normal entre les pulls
      min_interval_s   : intervalle minimum (floor)
      max_interval_s   : intervalle maximum après backoff complet
      backoff_factor   : multiplicateur à chaque cycle vide
      jitter_pct       : ±% de bruit aléatoire sur l'intervalle
    """

    def __init__(
        self,
        base_interval_s: float = 300.0,
        min_interval_s: float = 30.0,
        max_interval_s: float = 3600.0,
        backoff_factor: float = 1.5,
        jitter_pct: float = 0.1,
    ) -> None:
        self._base = base_interval_s
        self._min = min_interval_s
        self._max = max_interval_s
        self._factor = backoff_factor
        self._jitter_pct = jitter_pct
        self._current = base_interval_s
        self._consecutive_empty = 0

    async def schedule(
        self,
        job: Callable[[], Awaitable[JobResult]],
    ) -> AsyncIterator[JobResult]:
        while True:
            result = await job()
            yield result
            interval = self._jittered_interval()
            logger.debug(
                "Polling: next cycle in %.1fs (empty_streak=%d)",
                interval, self._consecutive_empty,
            )
            await asyncio.sleep(interval)

    def backoff(self) -> None:
        self._consecutive_empty += 1
        self._current = min(self._current * self._factor, self._max)
        logger.debug("Polling backoff → %.1fs (streak=%d)", self._current, self._consecutive_empty)

    def reset(self) -> None:
        if self._consecutive_empty > 0:
            logger.debug(
                "Polling reset → %.1fs (was %.1fs after %d empty cycles)",
                self._base, self._current, self._consecutive_empty,
            )
        self._current = self._base
        self._consecutive_empty = 0

    def current_interval_s(self) -> float:
        return self._current

    def _jittered_interval(self) -> float:
        jitter = self._current * self._jitter_pct * random.uniform(-1, 1)
        return max(self._min, self._current + jitter)
