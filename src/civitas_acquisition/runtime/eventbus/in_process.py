"""
InProcessEventBus — implémentation in-process du EventBusPort.

Pas de broker réseau. Pas de Kafka. Pas de Redis.
Pub/sub Python pur, in-process, async.

Garanties :
  - Livraison dans l'ordre d'enregistrement des handlers
  - Erreurs de handler isolées (loggées, ne propagent pas)
  - Thread-safe via asyncio
  - Subscriptions typées par classe d'événement
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Callable, Awaitable

from civitas_acquisition.contracts.ports.event_bus_port import (
    EventBusPort,
    EventHandler,
    Subscription,
)
from civitas_acquisition.contracts.models.events import AcquisitionEvent

logger = logging.getLogger(__name__)


class InProcessEventBus(EventBusPort):
    """
    Bus d'événements in-process.
    Utilisé comme infrastructure de découplage dans la Composition Root.

    Usage :
        bus = InProcessEventBus()
        sub = bus.subscribe(RawDocumentCreated, handler)
        await bus.emit(RawDocumentCreated(document_id="abc", ...))
        bus.unsubscribe(sub)
    """

    def __init__(self) -> None:
        # event_type → list de (subscription_id, handler)
        self._handlers: dict[type, list[tuple[str, EventHandler]]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def emit(self, event: AcquisitionEvent) -> None:
        """
        Émet un événement à tous les handlers enregistrés pour son type exact
        et ses types parents (héritage d'événements supporté).
        """
        handlers_to_call: list[EventHandler] = []

        # Collecter les handlers pour le type exact + types parents
        for event_type, handlers in self._handlers.items():
            if isinstance(event, event_type):
                handlers_to_call.extend(h for _, h in handlers)

        for handler in handlers_to_call:
            try:
                await handler(event)
            except Exception as exc:
                logger.error(
                    "EventBus handler error for %s: %s",
                    type(event).__name__,
                    exc,
                    exc_info=True,
                )

    def subscribe(
        self,
        event_type: type,
        handler: EventHandler,
    ) -> Subscription:
        sub = Subscription(event_type=event_type)
        self._handlers[event_type].append((sub.subscription_id, handler))
        logger.debug(
            "Subscribed to %s (id=%s)", event_type.__name__, sub.subscription_id[:8]
        )
        return sub

    def unsubscribe(self, subscription: Subscription) -> None:
        event_type = subscription.event_type
        if event_type in self._handlers:
            self._handlers[event_type] = [
                (sid, h)
                for sid, h in self._handlers[event_type]
                if sid != subscription.subscription_id
            ]
        logger.debug("Unsubscribed %s", subscription.subscription_id[:8])

    def subscriber_count(self, event_type: type) -> int:
        """Retourne le nombre de handlers enregistrés pour un type d'événement."""
        return len(self._handlers.get(event_type, []))

    def clear(self) -> None:
        """Retire tous les handlers. Utile pour les tests."""
        self._handlers.clear()
