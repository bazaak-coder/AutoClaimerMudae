"""Credential-provider interface.

Credentials (for example Discord account tokens) must never be stored in
plaintext JSON configuration. Configuration files are intentionally
credential-free: :mod:`autoclaimer.config` rejects credential-like keys.

This module defines the interface a secure credential provider implements so
later phases (worker orchestration, task execution) can request credentials
without knowing where they come from (environment variables, system keyring,
a secrets manager, ...).

No credential storage or secret-management implementation is provided here
by design; implement this interface with a backend appropriate to the
deployment environment.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class CredentialProviderError(RuntimeError):
    """Raised when a credential provider cannot supply a credential."""


@runtime_checkable
class CredentialProvider(Protocol):
    """Source of per-account credentials, keyed by Discord snowflake ID."""

    def get_token(self, account_id: int) -> str:
        """Return the secret token for ``account_id``.

        Implementations must raise :class:`CredentialProviderError` when the
        account is unknown or the credential cannot be retrieved, and must
        never log the returned secret.
        """
        ...

    def close(self) -> None:
        """Release provider resources. Must be safe to call more than once."""
        ...
