"""
EnvVault — implémentation du CredentialVaultPort via variables d'environnement.

UNIQUEMENT pour le développement et les tests.
Ne jamais utiliser en production.

Convention de nommage des variables d'environnement :
  path "acquisition/inst-github-1/token"
  → variable CIVITAS_ACQUISITION_INST_GITHUB_1_TOKEN

Les slashes et tirets deviennent des underscores. Tout en majuscules.
Préfixe : CIVITAS_
"""
from __future__ import annotations

import os
import re

from civitas_acquisition.contracts.ports.vault_port import CredentialVaultPort, SecretValue
from civitas_acquisition.contracts.errors.resilience_errors import VaultSecretNotFoundError


def _path_to_env_var(path: str) -> str:
    """Convertit un chemin vault en nom de variable d'environnement."""
    normalized = re.sub(r"[^a-zA-Z0-9]", "_", path).upper()
    return f"CIVITAS_{normalized}"


class EnvVault(CredentialVaultPort):
    """
    Vault basé sur les variables d'environnement.
    Pour dev et CI uniquement.
    """

    async def get_secret(self, path: str) -> SecretValue:
        env_var = _path_to_env_var(path)
        value = os.environ.get(env_var)
        if value is None:
            raise VaultSecretNotFoundError(path)
        return SecretValue(key=path.split("/")[-1], value=value)

    async def list_secrets(self, prefix: str) -> list[str]:
        env_prefix = _path_to_env_var(prefix)
        return [
            k for k in os.environ
            if k.startswith(env_prefix)
        ]

    async def set_secret(self, path: str, value: str) -> None:
        env_var = _path_to_env_var(path)
        os.environ[env_var] = value

    async def delete_secret(self, path: str) -> None:
        env_var = _path_to_env_var(path)
        os.environ.pop(env_var, None)

    async def rotate_secret(self, path: str) -> SecretValue:
        raise NotImplementedError("EnvVault does not support secret rotation")
