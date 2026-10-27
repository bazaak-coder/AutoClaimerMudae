"""Concrete Discord adapter backed by ``discord.py-self`` (one client per account).

Verified against discord.py-self 2.1.0 source (installed package):

- ``Client.login(token)`` / ``Client.connect(*, reconnect=True)`` /
  ``Client.close()`` / ``Client.wait_until_ready()`` / ``Client.get_channel(id)``.
- ``Client.event(coro)`` registers a single ``on_<name>`` handler via
  ``setattr``; ``Client.dispatch`` schedules it with one task per event.
  This fork has **no** ``add_listener``/``remove_listener``.
- ``Messageable.send(...)``, ``Messageable.fetch_message(id)``.
- ``Message.id/content/author/components/created_at``.
- Components: ``ActionRow.children`` and ``Button.custom_id/.url/.message``
  with ``Button.click() -> str | Interaction`` (a ``str`` means the button
  is just a link and no API call was made).
- ``discord.http`` routes ``add_reaction(channel_id, message_id, emoji)`` and
  ``get_message(channel_id, message_id)``.
- Inbound events (state.py dispatch sites): ``on_message(message)``;
  ``on_reaction_add(reaction, user)`` — fired **only when the message is in
  the gateway cache** (``Reaction.message/emoji/count``);
  ``on_raw_reaction_add(raw)`` — fired for **every** reaction
  (``RawReactionActionEvent.message_id/user_id/channel_id/emoji/
  message_author_id``), dispatched before the cached rich event;
  ``on_raw_message_edit(raw)`` — fired for **every** edit
  (``RawMessageUpdateEvent.message`` + ``.cached_message | None``). The
  fork passes the same updated-message object to raw and rich edit
  events, so the raw bridge fully replaces ``on_message_edit``: one
  delivery per edit with ``before`` from the cache when available.
- Exceptions: ``HTTPException`` (``.status``), ``RateLimited``
  (``.retry_after``; *not* an ``HTTPException`` subclass in this fork),
  ``Forbidden``, ``NotFound``, ``DiscordServerError``, ``CaptchaRequired``,
  ``LoginFailure``, ``ConnectionClosed``, ``GatewayNotFound``, ``InvalidData``.

Event model: inbound events are normalized to framework types
(:class:`~autoclaimer.adapters.MessageCreatedEvent`,
``ReactionAddedEvent``, ``MessageEditedEvent`) and fan out through a
bounded :class:`~autoclaimer.adapters.EventDispatcher` (per-subscriber
queue + pump, drop-oldest backpressure, consumer exception isolation).
The runtime performs **no history polling**: ``Messageable.history`` is
never called and the adapter does not implement historical retrieval.

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
from datetime import UTC, datetime
from typing import Any

import discord

from .adapters import (
    AdapterError,
    ChannelMessage,
    CooldownError,
    EventConsumer,
    EventDispatcher,
    InteractionResult,
    MessageCreatedEvent,
    MessageEditedEvent,
    PermanentAdapterError,
    RawReactionAddedEvent,
    ReactionAddedEvent,
    RecoverableAdapterError,
    SentMessage,
)
from .credentials import CredentialProvider

logger = logging.getLogger(__name__)

_DEFAULT_JITTER_RANGE = (0.4, 1.2)


class DiscordAdapter:
    """One account's Discord client wrapped behind :class:`PlatformAdapter`."""

    def __init__(
        self,
        *,
        account_id: int,
        credentials: CredentialProvider,
        client: discord.Client | None = None,
        client_factory: Callable[[], Any] | None = None,
        jitter_range: tuple[float, float] = _DEFAULT_JITTER_RANGE,
        jitter: Callable[[float, float], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        event_queue_size: int = 64,
        startup_timeout: float = 60.0,
    ) -> None:
        self._account_id = account_id
        self._credentials = credentials
        # The library's Client is terminal after close(): a restart needs a
        # fresh instance. An injected client is consumed by the first start;
        # client_factory (or the default) supplies replacements afterwards.
        self._client_factory = client_factory or (lambda: discord.Client())
        self._client = client if client is not None else self._client_factory()
        self._jitter_range = jitter_range
        self._jitter = jitter or (lambda low, high: random.uniform(low, high))
        self._sleep = sleep or asyncio.sleep
        self._start_lock = asyncio.Lock()
        self._connect_task: asyncio.Task[None] | None = None
        self._connect_error: BaseException | None = None
        self._started = False
        self._bridges_registered = False
        self._closed = False
        self._startup_timeout = startup_timeout
        self._events = EventDispatcher(queue_size=event_queue_size, name=f"discord-{account_id}")

    @property
    def is_healthy(self) -> bool:
        """True when the adapter is usable: not closed and connected.

        A started adapter whose connection task has died is unhealthy until
        it restarts (``start()`` / any operation triggers recovery).
        """
        if self._closed:
            return False
        if not self._started:
            return True
        task = self._connect_task
        return task is not None and not task.done()

    def _reset_for_restart(self) -> None:
        """Discard the terminal client and lifecycle state for a retry."""
        self._client = self._client_factory()
        self._connect_task = None
        self._connect_error = None
        self._started = False
        self._bridges_registered = False

    # ----- event subscription -----

    def subscribe(self, consumer: EventConsumer) -> Callable[[], None]:
        """Subscribe to normalized inbound events for THIS account.

        Subscribers may register before ``start()``; events only flow once
        the gateway connection delivers them, so nothing is lost between
        subscription and startup.
        """
        return self._events.subscribe(consumer)

    # ----- lifecycle -----

    async def start(self) -> None:
        """Log in and connect; never hangs on early failure; restarts.

        Readiness races the connection task and a startup timeout: if the
        connection dies (or ends) before READY, its error surfaces as a
        typed adapter error; on timeout everything is cleaned up. Failed
        startups are retryable, and a ``start()`` on an adapter whose
        connection died after READY performs a full restart with a fresh
        client (the library's client is terminal after close()).
        """
        async with self._start_lock:
            if self._closed:
                raise AdapterError("adapter is closed")
            if self._started:
                task = self._connect_task
                if task is not None and not task.done():
                    return  # healthy and running
                # Fatal disconnect after READY: rebuild and reconnect.
                self._reset_for_restart()
            self._register_bridges()
            token = self._credentials.get_token(self._account_id)
            try:
                await self._client.login(token)
            except discord.DiscordException as exc:
                raise _translate(exc) from exc
            self._connect_error = None
            self._connect_task = asyncio.create_task(
                self._client.connect(reconnect=True),
                name=f"autoclaimer-discord-{self._account_id}",
            )
            self._connect_task.add_done_callback(self._connect_finished)
            if self._closed:  # close() raced us: abort inline
                await self._abort_startup(None)
                raise AdapterError("adapter closed during startup")
            self._started = True
            logger.info(
                "discord client connecting",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.start",
                },
            )
        await self._await_ready()

    async def _await_ready(self) -> None:
        """Race readiness against connection death and the startup timeout."""
        assert self._connect_task is not None
        ready_task = asyncio.create_task(self._client.wait_until_ready())
        try:
            done, _pending = await asyncio.wait(
                {ready_task, self._connect_task},
                timeout=self._startup_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            ready_task.cancel()
            raise
        if ready_task in done:
            ready_error = ready_task.exception()
            if ready_error is not None:
                await self._abort_startup(ready_task)
                raise _translate_error(ready_error)
            if self._closed:
                await self._abort_startup(ready_task)
                raise AdapterError("adapter closed during startup")
            return  # ready: connection task keeps running in the background
        if self._connect_task in done:
            # Connection ended before readiness (crash or early return).
            # Capture the error BEFORE _abort_startup resets it.
            connect_error = self._connect_error
            error: BaseException = (
                connect_error
                if connect_error is not None
                else _UncleanExit("connection ended before the gateway reached READY")
            )
            await self._abort_startup(ready_task)
            raise _translate_error(error)
        # Timeout: neither readiness nor failure — abort cleanly.
        await self._abort_startup(ready_task)
        raise AdapterError(f"discord startup timed out after {self._startup_timeout:.1f}s")

    async def _abort_startup(self, ready_task: asyncio.Task[None] | None) -> None:
        """Tear down a failed startup so ``start()`` can be retried."""
        if ready_task is not None:
            ready_task.cancel()
        connect_task = self._connect_task
        self._connect_task = None
        self._started = False
        self._connect_error = None
        if connect_task is not None and not connect_task.done():
            connect_task.cancel()
            try:
                await connect_task
            except (asyncio.CancelledError, discord.DiscordException):
                pass
        try:
            await self._client.close()
        except discord.DiscordException:
            pass
        # The library client is terminal after close(): discard it so the
        # next start() builds a fresh one via the factory.
        self._client = self._client_factory()
        self._bridges_registered = False
        logger.warning(
            "discord startup aborted; retryable",
            extra={
                "account_id": self._account_id,
                "operation": "discord.start",
                "outcome": "startup_aborted",
            },
        )

    def _register_bridges(self) -> None:
        """Attach inbound-event bridges once; reconnects never duplicate."""
        if self._bridges_registered:
            return
        # The fork registers a single on_<name> handler per event via
        # Client.event(), which uses the coroutine's __name__. The adapter
        # owns its client, so no other consumer is displaced; the client
        # object persists across reconnects, keeping registration one-time.
        for event_name, handler in (
            ("on_message", self._on_message),
            ("on_reaction_add", self._on_reaction_add),
            ("on_raw_reaction_add", self._on_raw_reaction_add),
            ("on_raw_message_edit", self._on_raw_message_edit),
        ):
            self._client.event(_named_bridge(event_name, handler))
        self._bridges_registered = True

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
        if not self._started or self._connect_task is None or self._connect_task.done():
            # Not started, or the connection died: (re)start with a fresh
            # client — bounded by the startup race, never a bare wait.
            await self.start()
            return
        try:
            await asyncio.wait_for(self._client.wait_until_ready(), timeout=self._startup_timeout)
        except TimeoutError as exc:
            raise RecoverableAdapterError(
                f"discord gateway not ready within {self._startup_timeout:.1f}s"
            ) from exc
        if self._closed:
            raise AdapterError("adapter is closed")

    async def close(self) -> None:
        """Close the client, stop event delivery, and await tasks; idempotent.

        Outer cancellation during teardown completes the synchronous cleanup
        first and then re-raises, so teardown is never abandoned halfway.
        """
        if self._closed:
            return
        self._closed = True
        cancelled = False
        try:
            await self._events.stop()
        finally:
            try:
                await self._client.close()
            finally:
                task = self._connect_task
                if task is not None and not task.done():
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
                    except asyncio.CancelledError:
                        cancelled = True
                        task.cancel()
                        try:
                            await asyncio.shield(task)
                        except (asyncio.CancelledError, discord.DiscordException):
                            pass
                    except TimeoutError:
                        task.cancel()
                        try:
                            await task
                        except (asyncio.CancelledError, discord.DiscordException):
                            pass
        logger.info(
            "discord client closed",
            extra={"account_id": self._account_id, "operation": "discord.close"},
        )
        if cancelled:
            raise asyncio.CancelledError

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

    # ----- inbound event bridges -----

    async def _on_message(self, message: Any) -> None:
        """Bridge: ``on_message`` -> normalized MessageCreatedEvent."""
        try:
            await self._events.dispatch(
                MessageCreatedEvent(
                    account_id=self._account_id,
                    received_at=datetime.now(UTC),
                    message=_to_channel_message(message),
                )
            )
        except Exception:
            # A malformed event must never break delivery to subscribers.
            logger.exception(
                "failed to normalize message event",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.on_message",
                    "outcome": "dropped",
                },
            )

    async def _on_reaction_add(self, reaction: Any, user: Any) -> None:
        """Bridge: ``on_reaction_add`` (cached messages only in this fork)."""
        try:
            message_obj = getattr(reaction, "message", None)
            await self._events.dispatch(
                ReactionAddedEvent(
                    account_id=self._account_id,
                    received_at=datetime.now(UTC),
                    channel_id=message_obj.channel.id if message_obj is not None else 0,
                    message_id=message_obj.id if message_obj is not None else 0,
                    user_id=getattr(user, "id", 0),
                    emoji=_emoji_name(getattr(reaction, "emoji", "")),
                    message=(_to_channel_message(message_obj) if message_obj is not None else None),
                )
            )
        except Exception:
            logger.exception(
                "failed to normalize reaction event",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.on_reaction_add",
                    "outcome": "dropped",
                },
            )

    async def _on_raw_reaction_add(self, payload: Any) -> None:
        """Bridge: ``on_raw_reaction_add`` — fires for EVERY reaction.

        Complements the cached-only rich event; the fork dispatches raw
        before rich, so a cached reaction reaches consumers twice (raw
        then rich) unless they subscribe selectively.
        """
        try:
            await self._events.dispatch(
                RawReactionAddedEvent(
                    account_id=self._account_id,
                    received_at=datetime.now(UTC),
                    channel_id=payload.channel_id,
                    message_id=payload.message_id,
                    user_id=payload.user_id,
                    emoji=_emoji_name(payload.emoji),
                    message_author_id=getattr(payload, "message_author_id", None),
                )
            )
        except Exception:
            logger.exception(
                "failed to normalize raw reaction event",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.on_raw_reaction_add",
                    "outcome": "dropped",
                },
            )

    async def _on_raw_message_edit(self, payload: Any) -> None:
        """Bridge: ``on_raw_message_edit`` — covers cached AND uncached edits.

        The fork constructs the updated message once and passes it to both
        the raw and rich events, so this bridge fully replaces
        ``on_message_edit``: ``before`` comes from ``cached_message``
        (None when uncached), ``after`` from ``message``. Exactly one
        delivery per edit, no duplicates.
        """
        try:
            cached = getattr(payload, "cached_message", None)
            await self._events.dispatch(
                MessageEditedEvent(
                    account_id=self._account_id,
                    received_at=datetime.now(UTC),
                    before=_to_channel_message(cached) if cached is not None else None,
                    after=_to_channel_message(payload.message),
                )
            )
        except Exception:
            logger.exception(
                "failed to normalize message-edit event",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.on_raw_message_edit",
                    "outcome": "dropped",
                },
            )


class _UncleanExit(Exception):
    """Internal marker: the connection task ended without a library error."""


def _translate_error(error: BaseException) -> AdapterError:
    """Translate any startup failure into a typed adapter error."""
    if isinstance(error, discord.DiscordException):
        return _translate(error)
    return RecoverableAdapterError(f"discord startup failed: {type(error).__name__}: {error}")


def _named_bridge(event_name: str, handler: Callable[..., Awaitable[None]]) -> Any:
    """Wrap *handler* in a coroutine whose __name__ matches the event.

    The fork's ``Client.event()`` stores handlers by ``coro.__name__``,
    so the wrapper must be named exactly like the event.
    """

    async def bridge(*args: Any) -> None:
        await handler(*args)

    bridge.__name__ = event_name
    return bridge


def _to_channel_message(message: Any) -> ChannelMessage:
    author = getattr(message, "author", None)
    return ChannelMessage(
        id=message.id,
        channel_id=getattr(getattr(message, "channel", None), "id", 0),
        author_id=author.id if author is not None else 0,
        content=message.content or "",
        has_components=bool(getattr(message, "components", None)),
        created_at=getattr(message, "created_at", None),
    )


def _emoji_name(emoji: Any) -> str:
    """Normalize unicode/custom emoji objects to their string form."""
    if isinstance(emoji, str):
        return emoji
    return str(emoji)


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
