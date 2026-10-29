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
- Fatal account errors (owner policy 2026-10-28): the fork defines
  ``AuthFailure = LoginFailure`` (one class, two names) and raises it when
  ``login()`` rejects the token (HTTP 401). ``Client.connect(reconnect=
  True)`` only lets ``ConnectionClosed`` escape after a terminal
  ``close()`` and for close codes it cannot handle — the gateway's
  ``_can_handle_close`` treats ``1000`` (clean) and ``4004, 4010-4014``
  (auth/intent/shard/API-version fatal) as non-reconnectable. Both map to
  :class:`~autoclaimer.adapters.AccountFatalError`: the adapter latches
  the error permanently (no re-login, no client rebuild, ``is_healthy``
  false) and the worker manager stops the account. CAPTCHA challenges are
  equally account-fatal (never bypassed). All other ``ConnectionClosed``
  codes stay recoverable: the library already reconnected/resumed
  internally before anything surfaced.

Event model: inbound events are normalized to framework types
(:class:`~autoclaimer.adapters.MessageCreatedEvent`,
``ReactionAddedEvent``, ``MessageEditedEvent`) and fan out through a
bounded :class:`~autoclaimer.adapters.EventDispatcher` (per-subscriber
queue + pump, drop-oldest backpressure, consumer exception isolation).
The runtime performs **no history polling**: ``Messageable.history`` is
never called and the adapter does not implement historical retrieval.

Behavioural rules:

- **Pacing**: a randomized delay precedes every mutating action. Sends
  use a dedicated, tighter humanization range — ``send_jitter_range``,
  default 0.203–0.567 s, configurable as
  ``settings.send_jitter_seconds: [low, high]`` in config.json (pass
  ``settings.send_jitter_seconds`` from the parsed config into the
  adapter factory). Reactions and component presses use the general
  ``jitter_range`` (default 0.4–1.2 s). This is conservative pacing for
  personal, non-abusive use — **not** detection evasion.
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
    AccountFatalError,
    AdapterError,
    ChannelInfo,
    ChannelMessage,
    CooldownError,
    EmbedText,
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
    UncertainOutcomeError,
)
from .credentials import CredentialProvider

logger = logging.getLogger(__name__)

_DEFAULT_JITTER_RANGE = (0.4, 1.2)

#: Dedicated humanization range for send_message (owner directive
#: 2026-10-28: "keep it minimal range 0.203-0.567"). Configurable via
#: ``settings.send_jitter_seconds`` in config.json.
_DEFAULT_SEND_JITTER_RANGE = (0.203, 0.567)

#: Gateway close codes the library itself refuses to reconnect on
#: (``DiscordWebSocket._can_handle_close``, discord.py-self 2.1.0).
#: 4004 = authentication failed (token invalidated mid-session),
#: 4010-4014 = invalid shard / sharding required / API version /
#: intents problems. ``connect()`` closes the client and re-raises
#: ``ConnectionClosed`` for exactly these — we adhere to that verdict.
_FATAL_CLOSE_CODES = frozenset({4004, 4010, 4011, 4012, 4013, 4014})


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
        send_jitter_range: tuple[float, float] | None = None,
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
        self._send_jitter_range = send_jitter_range or _DEFAULT_SEND_JITTER_RANGE
        self._jitter = jitter or (lambda low, high: random.uniform(low, high))
        self._sleep = sleep or asyncio.sleep
        self._start_lock = asyncio.Lock()
        self._connect_task: asyncio.Task[None] | None = None
        self._connect_error: BaseException | None = None
        self._fatal: AccountFatalError | None = None
        self._started = False
        self._bridges_registered = False
        self._closed = False
        self._startup_timeout = startup_timeout
        self._events = EventDispatcher(queue_size=event_queue_size, name=f"discord-{account_id}")

    @property
    def is_healthy(self) -> bool:
        """True when the adapter is usable: not closed, not fatal, connected.

        A started adapter whose connection task has died is unhealthy until
        it restarts (``start()`` / any operation triggers recovery). An
        adapter that hit an account-fatal error (bad credentials, fatal
        gateway close, CAPTCHA) is permanently unhealthy: it never
        restarts or re-logins; the manager must stop the account.
        """
        if self._closed or self._fatal is not None:
            return False
        if not self._started:
            return True
        task = self._connect_task
        return task is not None and not task.done()

    def _latch_fatal(self, error: AccountFatalError) -> None:
        """Permanently remember an account-fatal failure.

        Once latched, every later ``start()``/operation re-raises the
        error immediately — no client rebuild, no re-login, no network
        traffic. The latch survives ``_reset_for_restart`` by design.
        """
        if self._fatal is None:
            self._fatal = error
            logger.error(
                "discord adapter hit account-fatal error",
                extra={
                    "account_id": self._account_id,
                    "operation": "discord.fatal",
                    "outcome": "account_fatal",
                },
            )

    def _reset_for_restart(self) -> None:
        """Discard the terminal client and lifecycle state for a retry.

        The fatal latch is deliberately preserved: a client may be
        rebuildable, but dead credentials or an invalidated session are
        not the client's fault and must not trigger re-login churn.
        """
        self._client = self._client_factory()
        self._connect_task = None
        self._connect_error = None
        self._started = False
        self._bridges_registered = False

    # ----- event subscription -----

    def subscribe(
        self,
        consumer: EventConsumer,
        *,
        on_events_lost: Callable[[int], None] | None = None,
    ) -> Callable[[], None]:
        """Subscribe to normalized inbound events for THIS account.

        Subscribers may register before ``start()``; events only flow once
        the gateway connection delivers them, so nothing is lost between
        subscription and startup. ``on_events_lost`` is the loss/
        reconciliation signal (milestone 5; forwarded verbatim).
        """
        return self._events.subscribe(consumer, on_events_lost=on_events_lost)

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
            if self._fatal is not None:
                raise self._fatal  # account-fatal: never re-login
            if self._started:
                task = self._connect_task
                if task is not None and not task.done():
                    return  # healthy and running
                # A fatal disconnect after READY latches in
                # _connect_finished / _await_ready; only non-fatal deaths
                # (transient close codes, network loss) reach this rebuild.
                self._reset_for_restart()
            self._register_bridges()
            token = self._credentials.get_token(self._account_id)
            try:
                await self._client.login(token)
            except discord.DiscordException as exc:
                translated = _translate(exc)
                if isinstance(translated, AccountFatalError):
                    self._latch_fatal(translated)
                raise translated from exc
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
            translated = _translate_error(error)
            if isinstance(translated, AccountFatalError):
                self._latch_fatal(translated)
            raise translated
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
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return
        self._connect_error = error
        # Latch fatal gateway verdicts the moment the connection dies
        # (post-READY invalidation): is_healthy flips false immediately
        # and the next operation fast-fails without any re-login.
        translated = _translate_error(error)
        if isinstance(translated, AccountFatalError):
            self._latch_fatal(translated)
        logger.warning(
            "discord connection task failed",
            extra={
                "account_id": self._account_id,
                "operation": "discord.connect",
                "outcome": "account_fatal"
                if isinstance(translated, AccountFatalError)
                else "connection_lost",
            },
        )

    async def _ensure_ready(self) -> None:
        if self._closed:
            raise AdapterError("adapter is closed")
        if self._fatal is not None:
            raise self._fatal  # account-fatal: no restart, no re-login
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

    async def _pace(self, jitter_range: tuple[float, float] | None = None) -> None:
        """Randomized conservative delay before a mutating action.

        ``send_message`` passes its dedicated, tighter humanization range
        (``send_jitter_range``); reactions and component presses use the
        general ``jitter_range``.
        """
        delay = self._jitter(*(jitter_range or self._jitter_range))
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

    async def get_channel(self, channel_id: int) -> ChannelInfo | None:
        """Cached channel info, or ``None`` when the account cannot see it.

        Uses the library's synchronous ``Client.get_channel`` (gateway
        cache) exclusively — ``fetch_channel`` (REST) is API-extensive
        over user accounts and is never called. Lazily ensures the
        gateway connection (the cache only exists after READY).
        """
        await self._ensure_ready()
        channel = self._client.get_channel(channel_id)
        if channel is None:
            return None
        return _to_channel_info(channel)

    async def validate_channel(self, channel_id: int) -> bool:
        """True when *channel_id* is visible to this account (cache-only)."""
        return await self.get_channel(channel_id) is not None

    # ----- PlatformAdapter operations -----

    async def send_message(self, channel_id: int, content: str) -> SentMessage:
        await self._ensure_ready()
        await self._pace(self._send_jitter_range)
        channel = self._channel(channel_id)
        try:
            message = await channel.send(content)
        except discord.DiscordException as exc:
            # Mutating call: a 5xx or unknown error leaves the outcome
            # ambiguous — UncertainOutcomeError, never auto-retried.
            raise _translate(exc, mutating=True) from exc
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
            # Mutating call: ambiguous failures are never auto-retried.
            raise _translate(exc, mutating=True) from exc

    async def press_component(
        self, channel_id: int, message_id: int, component_id: str
    ) -> InteractionResult:
        await self._ensure_ready()
        await self._pace()
        channel = self._channel(channel_id)
        try:
            message = await channel.fetch_message(message_id)
        except discord.DiscordException as exc:
            # READ: a 5xx response proves nothing was mutated — safe to
            # retry within bounds.
            raise _translate(exc) from exc
        button = _find_button(message, component_id)
        if button is None:
            raise PermanentAdapterError(
                f"component {component_id!r} not found on message {message_id}"
            )
        try:
            result = await button.click()
        except discord.DiscordException as exc:
            # Mutating call: ambiguous failures are never auto-retried.
            raise _translate(exc, mutating=True) from exc
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
        if not self._events.has_subscribers:
            return  # audit perf: skip normalization when nobody listens
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
        embeds=_embed_texts(getattr(message, "embeds", None)),
    )


def _embed_texts(embeds: Any) -> tuple[EmbedText, ...]:
    """Harvest per-field embed text (nested library and flat DTO shapes).

    Only text crosses the boundary; non-text embed data (colors, images,
    fields arrays) is deliberately dropped — parsing needs words.
    """

    def text(value: Any) -> str | None:
        return value if isinstance(value, str) and value.strip() else None

    harvested: list[EmbedText] = []
    if embeds is not None and not hasattr(embeds, "__iter__"):
        embeds = ()  # non-iterable attr: degrade, never drop the message (audit F15)
    for embed in embeds or ():
        embed_author = getattr(embed, "author", None)
        embed_footer = getattr(embed, "footer", None)
        entry = EmbedText(
            author_name=text(
                getattr(embed_author, "name", None) or getattr(embed, "author_name", None)
            ),
            title=text(getattr(embed, "title", None)),
            description=text(getattr(embed, "description", None)),
            footer=text(getattr(embed_footer, "text", None) or getattr(embed, "footer_text", None)),
        )
        if any((entry.author_name, entry.title, entry.description, entry.footer)):
            harvested.append(entry)
    return tuple(harvested)


def _to_channel_info(channel: Any) -> ChannelInfo:
    """Normalize a cached library channel object (never leaked as-is).

    DM channels may carry no ``name``; fall back to the recipient's
    name. The type enum is reduced to its plain name.
    """
    channel_type = getattr(channel, "type", None)
    type_name = getattr(channel_type, "name", None) or str(channel_type)
    name = getattr(channel, "name", None)
    if not name:
        recipient = getattr(channel, "recipient", None)
        name = getattr(recipient, "name", "") if recipient is not None else ""
    return ChannelInfo(channel_id=channel.id, name=name or "", channel_type=type_name)


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


def _translate(exc: discord.DiscordException, *, mutating: bool = False) -> AdapterError:
    """Map library exceptions to the framework's typed error model.

    ``mutating`` tells the translator whether the failing call could have
    applied a platform side effect. A completed 5xx response (or an
    unrecognized library error) does not prove a mutation was NOT
    applied — for mutating operations it maps to
    :class:`UncertainOutcomeError` (never auto-retried; PR #2 review
    fix). Read-only calls and pre-mutation failures keep their safe
    bounded-retry classification.

    Account-fatal verdicts (dead credentials, fatal gateway close codes,
    CAPTCHA) map to :class:`AccountFatalError`, which stops the whole
    account instead of failing one task (owner policy 2026-10-28).
    """
    if isinstance(exc, discord.RateLimited):
        # 429 = the request was REJECTED, not applied: safe to retry for
        # mutating operations too, after the recorded cooldown.
        return CooldownError(exc.retry_after)
    if isinstance(exc, discord.CaptchaRequired):
        return AccountFatalError(
            "Discord requested a CAPTCHA challenge; the account is stopped — "
            "challenges are never bypassed"
        )
    if isinstance(exc, discord.LoginFailure):
        # AuthFailure is an alias of LoginFailure in this fork; the token
        # was rejected — retrying it can only hammer the login endpoint.
        return AccountFatalError(f"authentication failed: {exc}")
    if isinstance(exc, (discord.Forbidden, discord.NotFound)):
        return PermanentAdapterError(str(exc))
    if isinstance(exc, discord.DiscordServerError):
        # The request completed with a 5xx RESPONSE: whether Discord
        # applied the action is unknowable from here.
        if mutating:
            return UncertainOutcomeError(
                f"server error {exc.status}; the mutation may have been applied: {exc}"
            )
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
            if mutating:
                return UncertainOutcomeError(
                    f"HTTP {exc.status}; the mutation may have been applied: {exc}"
                )
            return RecoverableAdapterError(f"HTTP {exc.status}: {exc}")
        return PermanentAdapterError(f"HTTP {exc.status}: {exc}")
    if isinstance(exc, discord.InvalidData):
        # The library raised InvalidData when a button click got no response,
        # which does NOT mean the interaction failed: the action may have
        # been applied. Never auto-retry (uncertain outcome policy).
        return UncertainOutcomeError(f"uncertain interaction outcome: {exc}")
    if isinstance(exc, discord.ConnectionClosed):
        # Only codes the library itself refuses to reconnect on ever get
        # here (gateway._can_handle_close): 4004 = token invalidated
        # mid-session, 4010-4014 = shard/intents/API-version fatal.
        if exc.code in _FATAL_CLOSE_CODES:
            return AccountFatalError(
                f"gateway session invalidated (close code {exc.code}): {exc.reason or exc}"
            )
        if mutating:
            # The connection failed around a mutating call — the platform
            # may have received and applied it before the closure. The
            # outcome is unknown: never auto-retry (same policy as 5xx).
            return UncertainOutcomeError(
                f"connection closed (code {exc.code}) during a mutation; "
                f"the outcome is unknown: {exc}"
            )
        return RecoverableAdapterError(f"connection problem: {exc}")
    if isinstance(exc, discord.GatewayNotFound):
        if mutating:
            return UncertainOutcomeError(
                f"gateway unavailable during a mutation; the outcome is unknown: {exc}"
            )
        return RecoverableAdapterError(f"connection problem: {exc}")
    # Unknown library errors: for mutating operations the outcome is
    # unknown (agents.md section 17.3 — never assume retry safety from
    # surprise); elsewhere bounded retries decide the outcome.
    if mutating:
        return UncertainOutcomeError(
            f"untranslated discord error; the mutation outcome is unknown: {exc}"
        )
    return RecoverableAdapterError(f"untranslated discord error: {exc}")
