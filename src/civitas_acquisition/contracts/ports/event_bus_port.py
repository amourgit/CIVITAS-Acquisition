"""
EventBusPort — interface abstraite du bus d'événements in-process.

Ce n'est PAS un message broker réseau.
C'est un pub/sub in-process, synchrone en ordre de livraison, async en exécution.

Il découple le pipeline des consommateurs d'événements
(plateforme downstream, monitoring, health checks) sans aucun appel réseau.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Awaitable, Callable
from uuid import uuid4

from ..models.events import AcquisitionEvent

EventHandler = Callable[[AcquisitionEvent], Awaitable[None]]


@dataclass
class Subscription:
    """
    Handle retourné par subscribe().
    Conserver et passer à unsubscribe() pour se désabonner.
    """
    subscription_id: str
    event_type: type

    def __init__(self, event_type: type) -> None:
        self.subscription_id = str(uuid4())
        self.event_type = event_type

    def __repr__(self) -> str:
        return (
            f"Subscription("
            f"id={self.subscription_id[:8]}..., "
            f"event_type={self.event_type.__name__!r})"
        )


class EventBusPort(ABC):
    """
    Interface abstraite pour le bus d'événements in-process.

    Usage :
        # Émettre
        await bus.emit(RawDocumentCreated(document_id=..., ...))

        # S'abonner
        sub = bus.subscribe(RawDocumentCreated, my_handler)

        # Se désabonner
        bus.unsubscribe(sub)
    """

    @abstractmethod
    async def emit(self, event: AcquisitionEvent) -> None:
        """
        Émet un événement à tous les handlers enregistrés pour son type.
        Les handlers sont appelés séquentiellement dans l'ordre d'enregistrement.
        Les erreurs de handler sont loggées mais ne propagent pas.
        """
        ...

    @abstractmethod
    def subscribe(
        self,
        event_type: type,
        handler: EventHandler,
    ) -> Subscription:
        """
        Enregistre un handler pour un type d'événement.
        Retourne un Subscription handle pour se désabonner plus tard.
        """
        ...

    @abstractmethod
    def unsubscribe(self, subscription: Subscription) -> None:
        """Retire un handler. Idempotent."""
        ...
