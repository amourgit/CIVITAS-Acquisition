"""
CredentialVaultPort — interface abstraite de gestion des secrets.

Implémentations : HashiCorp Vault, AWS Secrets Manager, Azure Key Vault,
                  EnvVault (dev uniquement).

La plateforme d'Acquisition ne stocke jamais de credentials en mémoire
au-delà du cycle de vie d'une connexion. Chaque connect() résout ses
credentials depuis le vault au moment de l'appel.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class SecretValue:
    """
    Valeur résolue d'un secret depuis le vault.
    Ne jamais logger ni sérialiser la valeur.
    """

    key: str
    value: str
    version: Optional[str] = None
    expires_at: Optional[datetime] = None

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now(tz=timezone.utc) > self.expires_at

    def __repr__(self) -> str:
        return f"SecretValue(key={self.key!r}, value=<redacted>, version={self.version!r})"

    def __str__(self) -> str:
        return self.__repr__()


class CredentialVaultPort(ABC):
    """
    Interface abstraite pour la gestion des secrets et credentials.

    Toutes les opérations sont async — les vaults sont des services réseau.
    Lève VaultSecretNotFoundError si le secret est absent.
    Lève VaultAccessDeniedError si les permissions sont insuffisantes.
    """

    @abstractmethod
    async def get_secret(self, path: str) -> SecretValue:
        """
        Récupère un secret par son chemin.
        path : "acquisition/{instance_id}/{credential_key}"
        """
        ...

    @abstractmethod
    async def list_secrets(self, prefix: str) -> list[str]:
        """Liste les chemins de secrets sous un préfixe."""
        ...

    @abstractmethod
    async def set_secret(self, path: str, value: str) -> None:
        """Crée ou met à jour un secret. Usage opérationnel seulement."""
        ...

    @abstractmethod
    async def delete_secret(self, path: str) -> None:
        """Supprime un secret définitivement."""
        ...

    @abstractmethod
    async def rotate_secret(self, path: str) -> SecretValue:
        """Déclenche la rotation d'un secret et retourne la nouvelle valeur."""
        ...
