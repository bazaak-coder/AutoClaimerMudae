"""Concrete Discord adapter backed by ``discord.py-self`` (one client per account).

Verified against discord.py-self 2.1.0 source (installed package):

- ``Client.login(token)`` / ``Client.connect(*, reconnect=True)`` /
  ``Client.close()`` / ``Client.wait_until_ready()`` / ``Client.get_channel(id)``
- ``Messageable.send(...)``, ``Messageable.fetch_message(id)``, and
  ``Messageable.history(*, limit=100, ...)``, which is a plain async
  generator in this fork (no ``.flatten()``).
- ``Message.id/content/author/components``, ``Message.add_reaction(emoji)``.
- Components: ``ActionRow.children`` and ``Button.custom_id/.url/.message``
  with ``Button.click() -> str | Interaction`` (a ``str`` means the button
  is just a link and no API call was made).
- ``discord.http`` routes ``add_reaction(channel_id, message_id, emoji)`` and
  ``get_message(channel_id, message_id)``.
- Exceptions: ``HTTPException`` (``.status``), ``RateLimited``
  (``.retry_after``; *not* an ``HTTPException`` subclass in this fork),
  ``Forbidden``, ``NotFound``, ``DiscordServerError``, ``CaptchaRequired``,
  ``LoginFailure``, ``ConnectionClosed``, ``GatewayNotFound``, ``InvalidData``.

Behavioural rules:

- **Pacing**: a small randomized delay (``jitter_range`` seconds) precedes
  every mutating action (send / react / press). This is conservative pacing
  to avoid hammering the API for personal use — it is not and must not
  become detection evasion.
- **Light endpoints only**: channels resolve from the gateway cache; the
  single moderate read used is ``get_message`` (required to obtain the
  component objects for a press). Heavy endpoints such as ``fetch_user``
  and ``fetch_channel`` are deliberately never called.
- **Rate limits**: the library already sleeps on most rate limits; anything
  that still surfaces (``RateLimited``, HTTP 429) is translated to
  :class:`~autoclaimer.adapters.CooldownError` with the server's
  ``retry_after`` so the central limiter records and honors it. CAPTCHA
  challenges translate to a permanent failure — they are never bypassed.
- **Credentials**: the token is fetched from the credential provider at
  login time, passed straight to ``Client.login``, and never stored,
  logged, or persisted by this adapter.
- **Lifecycle**: ``start()`` logs in, spawns ``connect(reconnect=True)`` as
  a background task, and awaits readiness; every operation lazily ensures
  readiness. If the connection task dies, operations fail recoverably
  until a restart. ``close()`` is idempotent and awaits task teardown.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any

import discord

from .adapters import (
    AdapterError,
    ChannelMessage,
    CooldownError,
    InteractionResult,
    PermanentAdapterError,
    RecoverableAdapterError,
    SentMessage,
)
from .credentials import CredentialProvider

logger = logging.getLogger(__name__)

_DEFAULT_JITTER_RANGE = (0.4, 1.2)


def _fake_response(status: int, reason: str = "") -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(status=status, reason=reason)


class DiscordAdapter:
    """One account's Discord client wrapped behind :class:`PlatformAdapter`."""

    def __init__(
        self,
        *,
        account_id: int,
        credentials: CredentialProvider,
        client: discord.Client | None = None,
        jitter_range: tuple[float, float] = _DEFAULT_JITTER_RANGE,
        jitter: Callable[[float, float], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._account_id = account_id
        self._credentials = credentials
        self._client = client if client is not None else discord.Client()
        self._jitter_range = jitter_range
        self._jitter = jitter or (lambda low, high: random.uniform(low, high))
        self._sleep = sleep or asyncio.sleep
        self._start_lock = asyncio.Lock()
        self._connect_task: asyncio.Task[None] | None = None
        self._connect_error: BaseException | None = None
        self._started = False
        self._closed = False

    # ----- lifecycle -----

    async def start(self) -> None:
        """Log in and connect; idempotent."""
        async with self._start_lock:
            if self._closed:
                raise AdapterError("adapter is closed")
            if self._started:
                return
            token = self._credentials.get_token(self._account_id)
            try:
                await self._client.login(token)
            except discord.DiscordException as exc:
                raise _translate(exc) from exc
            self._connect_task = asyncio.create_task(
                self._client.connect(reconnect=True),
                name=f"autoclaimer-discord-{self._account_id}",
            )
            self._connect_task.add_done_callback(self._connect_finished)
            self._started = True
            logger.info(
                "discord client connecting",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.start",
                },
            )
        await self._client.wait_until_ready()

    def _connect_finished(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            self._connect_error = task.exception()
            logger.warning(
                "discord connection task failed",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.connect",
                    "outcome": "connection_lost",
                },
            )

    async def _ensure_ready(self) -> None:
        if self._closed:
            raise AdapterError("adapter is closed")
        if self._connect_error is not None:
            raise RecoverableAdapterError(
                f"discord connection lost; restart the adapter "
                f"({type(self._connect_error).__name__})"
            )
        if not self._started:
            await self.start()
        else:
            await self._client.wait_until_ready()

    async def close(self) -> None:
        """Close the client and await the connection task; idempotent."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._client.close()
        finally:
            task = self._connect_task
            if task is not None and not task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
                except (TimeoutError, asyncio.CancelledError):
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, discord.DiscordException):
                        pass
        logger.info(
            "discord client closed",
            extra={"account_id": self._account_id, "operation": "discord.close"},
        )

    # ----- helpers -----

    async def _pace(self) -> None:
        """Randomized conservative delay before a mutating action."""
        delay = self._jitter(*self._jitter_range)
        if delay > 0:
            await self._sleep(delay)

    def _channel(self, channel_id: int) -> Any:
        channel = self._client.get_channel(channel_id)
        if channel is None:
            # Gateway cache miss: heavy fetch endpoints are deliberately
            # not used; the account must share a server with the channel.
            raise PermanentAdapterError(
                f"channel {channel_id} is not in the gateway cache; refusing "
                "to call fetch_channel (heavy endpoint)"
            )
        return channel

    # ----- PlatformAdapter operations -----

    async def send_message(self, channel_id: int, content: str) -> SentMessage:
        await self._ensure_ready()
        await self._pace()
        channel = self._channel(channel_id)
        try:
            message = await channel.send(content)
        except discord.DiscordException as exc:
            raise _translate(exc) from exc
        logger.debug(
            "message sent",
            extra={
                "account_id": self._account_id,
                "operation": "send_message",
                "outcome": "sent",
            },
        )
        return SentMessage(channel_id=channel_id, message_id=message.id, content=content)

    async def fetch_recent_messages(
        self, channel_id: int, limit: int = 50
    ) -> tuple[ChannelMessage, ...]:
        await self._ensure_ready()
        channel = self._channel(channel_id)
        try:
            messages = [message async for message in channel.history(limit=limit)]
        except discord.DiscordException as exc:
            raise _translate(exc) from exc
        # history() yields newest first; our contract is most recent last.
        messages.reverse()
        return tuple(
            ChannelMessage(
                id=message.id,
                channel_id=channel_id,
                author_id=message.author.id if message.author is not None else 0,
                content=message.content or "",
                has_components=bool(message.components),
            )
            for message in messages
        )

    async def add_reaction(self, channel_id: int, message_id: int, emoji: str) -> None:
        await self._ensure_ready()
        await self._pace()
        self._channel(channel_id)  # cache check only: no fetch_channel call
        try:
            await self._client.http.add_reaction(channel_id, message_id, emoji)
        except discord.DiscordException as exc:
            raise _translate(exc) from exc

    async def press_component(
        self, channel_id: int, message_id: int, component_id: str
    ) -> InteractionResult:
        await self._ensure_ready()
        await self._pace()
        channel = self._channel(channel_id)
        try:
            message = await channel.fetch_message(message_id)
            button = _find_button(message, component_id)
            if button is None:
                raise PermanentAdapterError(
                    f"component {component_id!r} not found on message {message_id}"
                )
            result = await button.click()
        except discord.DiscordException as exc:
            raise _translate(exc) from exc
        # A str result means the "button" is a link: no interaction occurred.
        acknowledged = not isinstance(result, str)
        return InteractionResult(
            message_id=message_id,
            component_id=component_id,
            acknowledged=acknowledged,
        )


def _find_button(message: Any, component_id: str) -> Any:
    for row in message.components or ():
        for child in getattr(row, "children", ()) or ():
            if getattr(child, "custom_id", None) == component_id:
                return child
    return None


def _translate(exc: discord.DiscordException) -> AdapterError:
    """Map library exceptions to the framework's typed error model."""
    if isinstance(exc, discord.RateLimited):
        return CooldownError(exc.retry_after)
    if isinstance(exc, discord.CaptchaRequired):
        return PermanentAdapterError(
            "Discord requested a CAPTCHA challenge; automation is stopped — "
            "challenges are never bypassed"
        )
    if isinstance(exc, (discord.Forbidden, discord.NotFound, discord.LoginFailure)):
        return PermanentAdapterError(str(exc))
    if isinstance(exc, discord.DiscordServerError):
        return RecoverableAdapterError(f"Discord server error ({exc.status}): {exc}")
    if isinstance(exc, discord.HTTPException):
        if exc.status == 429:
            retry = getattr(exc.response, "headers", None)
            retry_after = 1.0
            if retry is not None:
                try:
                    retry_after = float(retry.get("Retry-After", 1.0))
                except (TypeError, ValueError):
                    retry_after = 1.0
            return CooldownError(retry_after)
        if exc.status >= 500:
            return RecoverableAdapterError(f"HTTP {exc.status}: {exc}")
        return PermanentAdapterError(f"HTTP {exc.status}: {exc}")
    if isinstance(exc, discord.InvalidData):
        # The library raised InvalidData when a button click got no response,
        # which does NOT mean the interaction failed: outcome uncertain.
        return RecoverableAdapterError(f"uncertain interaction outcome: {exc}")
    if isinstance(exc, (discord.ConnectionClosed, discord.GatewayNotFound)):
        return RecoverableAdapterError(f"connection problem: {exc}")
    # Unknown library errors are treated as recoverable so bounded retries
    # (not silent success) decide the outcome.
    return RecoverableAdapterError(f"untranslated discord error: {exc}")
