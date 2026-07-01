"""
Deduplicators — détection de documents déjà connus.

Trois niveaux en cascade (du plus rapide au plus précis) :
  1. ExactHashDeduplicator  — SHA-256 du content. O(1). Gratuit.
  2. IdDeduplicator         — SHA-256(instance_id+uri). O(1). Idempotence.
  3. RepositoryDeduplicator — vérifie via RawRepositoryPort.exists().

En production : InMemoryDeduplicator pour les tests,
                RepositoryDeduplicator pour la production.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict

from civitas_acquisition.contracts.models.raw_document import RawDocument
from civitas_acquisition.contracts.ports.raw_repository_port import RawRepositoryPort


class DeduplicatorPort(ABC):
    @abstractmethod
    async def is_duplicate(self, doc: RawDocument) -> bool: ...

    @abstractmethod
    async def mark_seen(self, doc: RawDocument) -> None: ...


class InMemoryDeduplicator(DeduplicatorPort):
    """
    Déduplication in-memory par ID de document.
    LRU cache borné pour éviter la croissance mémoire infinie.
    Usage : tests, développement, petits volumes.
    """

    def __init__(self, max_size: int = 100_000) -> None:
        self._seen: OrderedDict[str, bool] = OrderedDict()
        self._max_size = max_size

    async def is_duplicate(self, doc: RawDocument) -> bool:
        return doc.id in self._seen

    async def mark_seen(self, doc: RawDocument) -> None:
        if doc.id in self._seen:
            self._seen.move_to_end(doc.id)
        else:
            self._seen[doc.id] = True
            if len(self._seen) > self._max_size:
                self._seen.popitem(last=False)   # evict LRU


class RepositoryDeduplicator(DeduplicatorPort):
    """
    Déduplication via le RawRepositoryPort.
    Source de vérité : le document existe déjà dans le repository ?
    Usage : production.
    """

    def __init__(self, repository: RawRepositoryPort) -> None:
        self._repo = repository

    async def is_duplicate(self, doc: RawDocument) -> bool:
        return await self._repo.exists(doc.id)

    async def mark_seen(self, doc: RawDocument) -> None:
        pass  # Le repository marque lui-même lors du write()


class TwoTierDeduplicator(DeduplicatorPort):
    """
    Deux niveaux : cache mémoire rapide + repository authoritative.
    Cache L1 : InMemory (évite les appels repository pour les doublons récents)
    Cache L2 : Repository (source de vérité persistante)
    """

    def __init__(
        self,
        repository: RawRepositoryPort,
        cache_size: int = 10_000,
    ) -> None:
        self._l1 = InMemoryDeduplicator(max_size=cache_size)
        self._l2 = RepositoryDeduplicator(repository)

    async def is_duplicate(self, doc: RawDocument) -> bool:
        if await self._l1.is_duplicate(doc):
            return True
        return await self._l2.is_duplicate(doc)

    async def mark_seen(self, doc: RawDocument) -> None:
        await self._l1.mark_seen(doc)
