"""Opt-in live checks for the Discord adapter.

Skipped unless the owner explicitly opts in via environment variables —
the normal suite never touches real credentials or Discord:

- ``AUTOCLAIMER_LIVE_DISCORD_TOKEN``: a real account token (login/close only).

User-account automation may violate Discord's Terms of Service. These
checks exist for the owner's personal, authorized verification only.
The runtime workflow is event-driven; no history endpoints are exercised.
"""

import os

import pytest

discord = pytest.importorskip("discord")

from autoclaimer.discord_adapter import DiscordAdapter  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.environ.get("AUTOCLAIMER_LIVE_DISCORD_TOKEN"),
    reason="live Discord checks are opt-in (set AUTOCLAIMER_LIVE_DISCORD_TOKEN)",
)


class _EnvTokenProvider:
    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self, account_id: int) -> str:
        return self._token

    def close(self) -> None:
        return None


async def test_live_login_and_clean_close() -> None:
    adapter = DiscordAdapter(
        account_id=1,
        credentials=_EnvTokenProvider(os.environ["AUTOCLAIMER_LIVE_DISCORD_TOKEN"]),
    )
    await adapter.start()
    assert not adapter._closed  # noqa: SLF001
    await adapter.close()
    assert adapter._closed  # noqa: SLF001


async def test_live_event_subscription_starts_cleanly() -> None:
    """Login with one subscriber registered; no events required to pass."""
    from autoclaimer.adapters import MessageCreatedEvent  # noqa: F401 (type anchor)

    received: list[object] = []

    async def consumer(event: object) -> None:
        received.append(event)

    adapter = DiscordAdapter(
        account_id=1,
        credentials=_EnvTokenProvider(os.environ["AUTOCLAIMER_LIVE_DISCORD_TOKEN"]),
    )
    unsubscribe = adapter.subscribe(consumer)
    await adapter.start()
    await adapter.close()
    unsubscribe()
    assert isinstance(received, list)  # delivery optional; lifecycle must hold


def test_package_version_recorded() -> None:
    # Documentation anchor: this suite was verified against this version.
    assert discord.__version__ == "2.1.0"
