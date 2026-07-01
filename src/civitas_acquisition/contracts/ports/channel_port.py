"""
ChannelPort — interface abstraite pour les canaux d'acquisition.

Un canal définit COMMENT la donnée entre dans le système.
Un canal est toujours couplé à un connecteur (le QUOI) et un pipeline (le OÙ).

Les 6 types de canaux :
- Polling  : pull périodique planifié par le Scheduler
- Webhook  : push inbound déclenché par la source
- Streaming: consommation continue (Kafka, Kinesis)
- Queue    : file de messages (AMQP, SQS)
- FileDropMonitoring: surveillance de répertoire
- Manual   : déclenchement one-shot par opérateur
"""

from __future__ import annotations
from abc import ABC, abstractmethod


class ChannelPort(ABC):
    """
    Interface abstraite pour tous les canaux d'acquisition.

    Un canal est responsable de la boucle d'acquisition —
    démarrer, écouter ou interroger, et s'arrêter proprement.
    Il délègue le traitement au pipeline.
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Démarre le canal.
        Pour Polling : lance la boucle de scheduling.
        Pour Webhook : démarre l'écoute HTTP.
        Pour Streaming : démarre le consumer.
        Bloque jusqu'à stop() pour les canaux continus.
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """
        Arrête le canal proprement.
        Attend que les traitements en cours se terminent avant de retourner.
        Doit être idempotent.
        """
        ...

    @abstractmethod
    def is_running(self) -> bool:
        """True si le canal est actif et traite des données."""
        ...
