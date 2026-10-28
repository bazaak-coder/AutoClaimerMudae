"""Unit tests for the discord.py-self adapter (fake client; no credentials)."""

import asyncio
from types import SimpleNamespace
from typing import Any

import discord
import pytest

from autoclaimer.adapters import (
    AccountFatalError,
    AdapterError,
    ChannelInfo,
    CooldownError,
    InteractionResult,
    PermanentAdapterError,
    RecoverableAdapterError,
    SentMessage,
    UncertainOutcomeError,
)
from autoclaimer.credentials import CredentialProviderError
from autoclaimer.discord_adapter import DiscordAdapter

ACCOUNT = 271485504361996288
CHANNEL = 999999000001111222
TOKEN = "user-account-token-value"  # fake; never a real credential


class FakeCredentials:
    def __init__(self, token: str = TOKEN, *, fail: bool = False) -> None:
        self.token = token
        self.fail = fail
        self.calls: list[int] = []

    def get_token(self, account_id: int) -> str:
        self.calls.append(account_id)
        if self.fail:
            raise CredentialProviderError("no credential available")
        return self.token

    def close(self) -> None:
        return None


class FakeButton:
    def __init__(self, custom_id: str, *, result: Any = None, error: Exception | None = None):
        self.custom_id = custom_id
        self.url = None
        self._result = result if result is not None else object()  # Interaction-like
        self._error = error

    async def click(self) -> Any:
        if self._error is not None:
            raise self._error
        return self._result


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        content: str = "",
        *,
        author_id: int = 111222333,
        components: list[Any] | None = None,
    ) -> None:
        self.id = message_id
        self.content = content
        self.author = SimpleNamespace(id=author_id)
        self.components = components or []

    async def add_reaction(self, emoji: str) -> None:
        return None


class FakeChannel:
    def __init__(
        self,
        channel_id: int,
        messages: list[FakeMessage],
        *,
        name: str = "test-channel",
        channel_type: str = "text",
    ) -> None:
        self.id = channel_id
        self.name = name
        self.type = SimpleNamespace(name=channel_type)  # ChannelType-like
        self._messages = messages
        self.sent: list[str] = []
        self.fetched: list[int] = []

    async def send(self, content: str) -> FakeMessage:
        message = FakeMessage(len(self.sent) + 1, content)
        self.sent.append(content)
        self._messages.append(message)
        return message

    async def fetch_message(self, message_id: int) -> FakeMessage:
        self.fetched.append(message_id)
        for message in self._messages:
            if message.id == message_id:
                return message
        raise discord.NotFound(SimpleNamespace(status=404, reason="Not Found"), "Unknown Message")


class FakeHttp:
    def __init__(self) -> None:
        self.reactions: list[tuple[int, int, str]] = []
        self.error: Exception | None = None

    async def add_reaction(self, channel_id: int, message_id: int, emoji: str) -> None:
        if self.error is not None:
            raise self.error
        self.reactions.append((channel_id, message_id, emoji))


class FakeClient:
    """Implements exactly the discord.Client surface the adapter touches."""

    def __init__(self, channel: FakeChannel | None = None) -> None:
        self._channel = channel
        self.login_calls: list[str] = []
        self.close_calls = 0
        self.ready = asyncio.Event()
        self.connect_started = asyncio.Event()
        self.connect_stopped = asyncio.Event()
        self.http = FakeHttp()
        self.closed_flag = False
        self.registered_events: list[str] = []
        self.history_accessed = 0

    def event(self, coro: Any) -> Any:
        """Mimics the fork's Client.event: setattr by coroutine name."""
        setattr(self, coro.__name__, coro)
        self.registered_events.append(coro.__name__)
        return coro

    async def history_trap(self) -> None:
        self.history_accessed += 1
        raise AssertionError("history() must never be called")

    async def login(self, token: str) -> None:
        self.login_calls.append(token)
        self.ready.set()

    async def connect(self, *, reconnect: bool = True) -> None:
        self.connect_started.set()
        await self.connect_stopped.wait()

    async def wait_until_ready(self) -> None:
        await self.ready.wait()

    async def close(self) -> None:
        self.close_calls += 1
        self.closed_flag = True
        self.connect_stopped.set()
        self.ready.set()

    def get_channel(self, channel_id: int) -> FakeChannel | None:
        if self._channel is not None and self._channel.id == channel_id:
            return self._channel
        return None

    async def fetch_channel(self, channel_id: int) -> Any:
        raise AssertionError("fetch_channel must never be called (API-extensive)")


def make_adapter(
    client: FakeClient,
    credentials: FakeCredentials | None = None,
    *,
    events: list[str] | None = None,
) -> DiscordAdapter:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if events is not None:
            events.append(f"sleep:{seconds}")

    adapter = DiscordAdapter(
        account_id=ACCOUNT,
        credentials=credentials or FakeCredentials(),
        client=client,  # type: ignore[arg-type]  # fake implements the used surface
        jitter=lambda low, high: 0.05,
        sleep=fake_sleep,
    )
    adapter.test_sleeps = sleeps  # type: ignore[attr-defined]
    return adapter


def button_message(*buttons: FakeButton, message_id: int = 55) -> FakeMessage:
    return FakeMessage(
        message_id,
        "roll",
        components=[SimpleNamespace(children=list(buttons))],
    )


# ----- lifecycle -----


async def test_start_logs_in_connects_and_waits_ready() -> None:
    client = FakeClient()
    credentials = FakeCredentials()
    adapter = make_adapter(client, credentials)
    await adapter.start()
    await asyncio.sleep(0)  # let the spawned connect task begin
    assert client.login_calls == [TOKEN]
    assert credentials.calls == [ACCOUNT]
    assert client.connect_started.is_set()
    await adapter.start()  # idempotent: no second login
    assert client.login_calls == [TOKEN]
    client.connect_stopped.set()
    await adapter.close()


async def test_close_is_idempotent_and_closes_client() -> None:
    client = FakeClient()
    adapter = make_adapter(client)
    await adapter.start()
    client.connect_stopped.set()
    await adapter.close()
    await adapter.close()
    assert client.close_calls == 1
    with pytest.raises(AdapterError, match="closed"):
        await adapter.send_message(1, "x")


async def test_operations_lazily_start_the_adapter() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    sent = await adapter.send_message(CHANNEL, "hello")
    assert client.login_calls == [TOKEN]  # started on demand
    assert sent == SentMessage(channel_id=CHANNEL, message_id=1, content="hello")
    client.connect_stopped.set()
    await adapter.close()


async def test_login_failure_is_account_fatal_latches_and_token_never_leaks() -> None:
    client = FakeClient()

    async def fail_login(token: str) -> None:
        raise discord.LoginFailure("Improper token passed.")

    client.login = fail_login  # type: ignore[method-assign]
    adapter = make_adapter(client)
    with pytest.raises(AccountFatalError, match="authentication failed.*Improper token"):
        await adapter.start()
    assert TOKEN not in repr(adapter)
    # The fatal verdict latches: no second login attempt, ever.
    assert adapter.is_healthy is False
    with pytest.raises(AccountFatalError, match="authentication failed"):
        await adapter.start()
    with pytest.raises(AccountFatalError, match="authentication failed"):
        await adapter.send_message(1, "x")
    assert client.login_calls == []  # fail_login raises before recording; no retries


# ----- pacing -----


async def test_random_delay_precedes_mutating_actions_not_inbound_events() -> None:
    from datetime import UTC, datetime

    from autoclaimer.adapters import MessageCreatedEvent

    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    received: list[object] = []
    adapter.subscribe(received.append)
    now = datetime.now(UTC)
    await adapter._on_message(
        SimpleNamespace(
            id=1,
            content="hi",
            author=SimpleNamespace(id=1),
            channel=SimpleNamespace(id=CHANNEL),
            components=[],
            created_at=now,
        )
    )  # inbound event: no jitter
    await asyncio.sleep(0.01)  # let the pump deliver
    assert len(received) == 1
    assert isinstance(received[0], MessageCreatedEvent)
    assert adapter.test_sleeps == []  # type: ignore[attr-defined]
    await adapter.send_message(CHANNEL, "hi")  # mutating action: jitter applied
    assert adapter.test_sleeps == [0.05]  # type: ignore[attr-defined]
    client.connect_stopped.set()
    await adapter.close()


async def test_cancellation_during_pacing_propagates_cleanly() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    adapter = make_adapter(client)

    async def hang(seconds: float) -> None:
        await asyncio.Event().wait()  # never resolves

    adapter._sleep = hang  # noqa: SLF001
    task = asyncio.create_task(adapter.send_message(CHANNEL, "x"))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert channel.sent == []  # nothing was sent
    client.connect_stopped.set()
    await adapter.close()


# ----- channel resolution and reads -----


async def test_unknown_channel_is_permanent_without_heavy_fetches() -> None:
    client = FakeClient(None)
    adapter = make_adapter(client)
    with pytest.raises(PermanentAdapterError, match="gateway cache"):
        await adapter.send_message(12345, "x")
    client.connect_stopped.set()
    await adapter.close()


# ----- startup reliability (PR #1 finding 1 regression tests) -----


class StartupFakeClient:
    """Controllable startup surface: ready is set explicitly by tests."""

    def __init__(self, *, connect_error: Exception | None = None) -> None:
        self.ready = asyncio.Event()
        self.connect_error = connect_error
        self.connect_runs = 0
        self.closed = False
        self.registered: list[str] = []
        self._stop = asyncio.Event()

    def event(self, coro: Any) -> Any:
        setattr(self, coro.__name__, coro)
        self.registered.append(coro.__name__)
        return coro

    async def login(self, token: str) -> None:
        return None

    async def connect(self, *, reconnect: bool = True) -> None:
        self.connect_runs += 1
        if self.connect_error is not None:
            raise self.connect_error
        await self._stop.wait()

    async def wait_until_ready(self) -> None:
        await self.ready.wait()

    async def close(self) -> None:
        self.closed = True
        self._stop.set()

    def get_channel(self, channel_id: int) -> None:
        return None


def make_gateway_not_found() -> discord.GatewayNotFound:
    return discord.GatewayNotFound()  # type: ignore[no-untyped-call]  # untyped ctor


def startup_adapter(client: StartupFakeClient, **kwargs: Any) -> DiscordAdapter:
    return DiscordAdapter(
        account_id=ACCOUNT,
        credentials=FakeCredentials(),
        client=client,  # type: ignore[arg-type]
        jitter=lambda low, high: 0.0,
        sleep=_no_sleep_async,
        **kwargs,
    )


async def _no_sleep_async(seconds: float) -> None:
    return None


async def test_early_connect_failure_fails_promptly_and_is_retryable() -> None:
    client = StartupFakeClient(connect_error=make_gateway_not_found())
    adapter = startup_adapter(client, client_factory=lambda: client)
    with pytest.raises(RecoverableAdapterError, match="GatewayNotFound|connection"):
        await adapter.start()  # pre-fix: hung forever
    assert client.closed is True  # cleanup happened
    # Retry with a now-working connection succeeds (fresh client via factory).
    client.connect_error = None
    client.ready.set()
    await adapter.start()
    client._stop.set()  # noqa: SLF001
    await adapter.close()


async def test_startup_timeout_cleans_up_without_leaks() -> None:
    client = StartupFakeClient()  # neither ready nor failure
    adapter = startup_adapter(client, startup_timeout=0.05)
    with pytest.raises(AdapterError, match="timed out"):
        await adapter.start()
    assert client.closed is True
    leaks = [
        t
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task() and "discord" in t.get_name()
    ]
    assert leaks == []
    await adapter.close()  # idempotent after abort


async def test_close_during_startup_fails_promptly() -> None:
    client = StartupFakeClient()
    adapter = startup_adapter(client)

    async def close_soon() -> None:
        await asyncio.sleep(0.01)
        client.ready.set()  # close() releases the readiness wait
        await adapter.close()

    closer = asyncio.create_task(close_soon())
    with pytest.raises(AdapterError, match="closed during startup"):
        await adapter.start()
    await closer


async def test_clean_early_connect_return_surfaces_error() -> None:
    client = StartupFakeClient()
    client._stop.set()  # noqa: SLF001 - connect() returns without READY
    adapter = startup_adapter(client)
    with pytest.raises(RecoverableAdapterError, match="before the gateway"):
        await adapter.start()
    assert client.closed is True


async def test_ready_wins_race_and_start_is_idempotent() -> None:
    client = StartupFakeClient()
    adapter = startup_adapter(client)
    client.ready.set()
    await adapter.start()
    await adapter.start()  # idempotent: no second connect
    assert client.connect_runs == 1
    client._stop.set()  # noqa: SLF001
    await adapter.close()


# ----- reactions via the light http route -----


async def test_add_reaction_uses_http_route_and_translates_rate_limit() -> None:
    channel = FakeChannel(CHANNEL, [FakeMessage(7)])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    await adapter.add_reaction(CHANNEL, 7, "🎲")
    assert client.http.reactions == [(CHANNEL, 7, "🎲")]

    client.http.error = discord.RateLimited(12.5)
    with pytest.raises(CooldownError) as excinfo:
        await adapter.add_reaction(CHANNEL, 7, "🎲")
    assert excinfo.value.retry_after_seconds == 12.5
    client.connect_stopped.set()
    await adapter.close()


# ----- component interactions -----


async def test_press_component_normalizes_interaction_result() -> None:
    interaction = SimpleNamespace(id="interaction-1")
    channel = FakeChannel(
        CHANNEL, [button_message(FakeButton("claim", result=interaction), message_id=55)]
    )
    client = FakeClient(channel)
    adapter = make_adapter(client)
    result = await adapter.press_component(CHANNEL, 55, "claim")
    assert result == InteractionResult(message_id=55, component_id="claim", acknowledged=True)
    client.connect_stopped.set()
    await adapter.close()


async def test_press_url_button_reports_not_acknowledged() -> None:
    channel = FakeChannel(
        CHANNEL,
        [button_message(FakeButton("link", result="https://example.com"), message_id=55)],
    )
    client = FakeClient(channel)
    adapter = make_adapter(client)
    result = await adapter.press_component(CHANNEL, 55, "link")
    assert result.acknowledged is False
    client.connect_stopped.set()
    await adapter.close()


async def test_press_missing_component_is_permanent() -> None:
    channel = FakeChannel(CHANNEL, [button_message(FakeButton("claim"), message_id=55)])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    with pytest.raises(PermanentAdapterError, match="not found"):
        await adapter.press_component(CHANNEL, 55, "other")
    client.connect_stopped.set()
    await adapter.close()


async def test_button_click_without_response_is_uncertain_never_retried() -> None:
    button = FakeButton("claim", error=discord.InvalidData("no response received"))
    channel = FakeChannel(CHANNEL, [button_message(button, message_id=55)])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    with pytest.raises(UncertainOutcomeError, match="uncertain"):
        await adapter.press_component(CHANNEL, 55, "claim")
    client.connect_stopped.set()
    await adapter.close()


# ----- error translation -----


async def test_get_channel_and_validate_channel_are_cache_only() -> None:
    """Consumer channel discovery: gateway cache only, never a REST call.

    ``fetch_channel`` (REST GET /channels/{id}) is API-extensive over
    user accounts and must never be reached; ``Client.get_channel`` is a
    synchronous cache lookup."""
    channel = FakeChannel(CHANNEL, [], name="mudae-claims", channel_type="text")
    client = FakeClient(channel)
    adapter = make_adapter(client)
    await adapter.start()

    info = await adapter.get_channel(CHANNEL)
    assert info == ChannelInfo(channel_id=CHANNEL, name="mudae-claims", channel_type="text")
    assert await adapter.validate_channel(CHANNEL) is True

    assert await adapter.get_channel(111222333444) is None
    assert await adapter.validate_channel(111222333444) is False

    client.connect_stopped.set()
    await adapter.close()


async def test_get_channel_lazily_starts_adapter() -> None:
    channel = FakeChannel(CHANNEL, [], name="any", channel_type="text")
    client = FakeClient(channel)
    adapter = make_adapter(client)
    assert await adapter.validate_channel(CHANNEL) is True  # started on demand
    assert client.login_calls == [TOKEN]
    client.connect_stopped.set()
    await adapter.close()


async def test_http_error_translation_matrix() -> None:
    cases: list[tuple[discord.DiscordException, type[AdapterError]]] = [
        (discord.RateLimited(9.0), CooldownError),
        (
            discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "no perms"),
            PermanentAdapterError,
        ),
        (
            discord.DiscordServerError(
                SimpleNamespace(status=503, reason="oops", text="unavailable"), "down"
            ),
            RecoverableAdapterError,
        ),
        (
            discord.CaptchaRequired(
                SimpleNamespace(status=428, reason="captcha"),
                {"captcha_key": ["x"], "captcha_sitekey": "site"},
            ),
            AccountFatalError,
        ),
        # AuthFailure is an alias of LoginFailure in this fork: the token
        # itself is dead — account-fatal, never retried (owner policy).
        (discord.LoginFailure("Improper token"), AccountFatalError),
        (discord.AuthFailure("Improper token"), AccountFatalError),
        # Fatal gateway close codes (gateway._can_handle_close): the
        # library already closed the client and gave up by the time these
        # surface — the account's session is dead.
        (discord.ConnectionClosed(4004, "authentication failed"), AccountFatalError),
        (discord.ConnectionClosed(4010, "invalid shard"), AccountFatalError),
        (discord.ConnectionClosed(4014, "disallowed intents"), AccountFatalError),
        # Everything else the library can resume/reconnect from stays
        # recoverable (code None = unknown/transport-level, 1000 clean,
        # 1006 abnormal, 4xxx unknown-but-reconnectable).
        (discord.ConnectionClosed(None), RecoverableAdapterError),
        (discord.ConnectionClosed(1000, "clean"), RecoverableAdapterError),
        (discord.ConnectionClosed(1006, "abnormal"), RecoverableAdapterError),
        (discord.ConnectionClosed(4000, "unknown"), RecoverableAdapterError),
        (discord.GatewayNotFound(), RecoverableAdapterError),  # type: ignore[no-untyped-call]
    ]
    from autoclaimer.discord_adapter import _translate

    for exc, expected in cases:
        translated = _translate(exc)
        assert type(translated) is expected, f"{exc!r} -> {type(translated)}"


async def test_fatal_gateway_close_after_ready_latches_without_relogin() -> None:
    """Session invalidated mid-session (owner scenario): Discord closes the
    gateway with 4004; the connect task dies; the adapter latches the
    fatal verdict and every later operation fast-fails — exactly one
    login attempt total, no client rebuild."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)

    async def invalidated_session(*, reconnect: bool = True) -> None:
        raise discord.ConnectionClosed(4004, "authentication failed.")

    client.connect = invalidated_session  # type: ignore[method-assign]
    adapter = make_adapter(client)
    await adapter.start()  # login + READY race: ready wins, connect dies after
    task = adapter._connect_task  # noqa: SLF001
    assert task is not None
    with pytest.raises(discord.ConnectionClosed):
        await task  # the library's fatal verdict escapes connect()
    await asyncio.sleep(0)  # let the done-callback latch
    assert adapter.is_healthy is False
    with pytest.raises(AccountFatalError, match="close code 4004"):
        await adapter.send_message(CHANNEL, "hi")
    with pytest.raises(AccountFatalError, match="close code 4004"):
        await adapter.start()
    assert client.login_calls == [TOKEN]  # exactly one login: no retry churn
    assert channel.sent == []  # nothing was sent on a dead session


async def test_captcha_never_bypassed_message_is_explicit() -> None:
    from autoclaimer.discord_adapter import _translate

    exc = discord.CaptchaRequired(
        SimpleNamespace(status=428, reason="captcha"),
        {"captcha_key": ["required"], "captcha_sitekey": "site"},
    )
    translated = _translate(exc)
    assert isinstance(translated, PermanentAdapterError)
    assert "never bypassed" in str(translated)


async def test_connection_death_triggers_restart_and_recovery() -> None:
    """Post-READY fatal disconnect: the next operation self-heals."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    adapter = DiscordAdapter(
        account_id=ACCOUNT,
        credentials=FakeCredentials(),
        client=client,  # type: ignore[arg-type]
        jitter=lambda low, high: 0.0,
        sleep=_no_sleep_async,
        client_factory=lambda: client,
    )
    await adapter.start()
    assert adapter.is_healthy is True
    # Simulate the gateway dying: connect task completes with an error.
    client.connect_stopped.set()
    adapter._connect_error = discord.ConnectionClosed(None)  # noqa: SLF001
    task = adapter._connect_task  # noqa: SLF001
    assert task is not None
    await asyncio.sleep(0)  # let the task observe the stop event
    assert adapter.is_healthy is False
    client.connect_stopped.clear()  # the restart blocks again on connect
    client.ready.set()
    sent = await adapter.send_message(CHANNEL, "after recovery")  # self-heals
    assert sent.content == "after recovery"
    assert adapter.is_healthy is True
    await adapter.close()


async def test_send_translates_forbidden_from_channel_send() -> None:
    class ForbiddenChannel(FakeChannel):
        async def send(self, content: str) -> FakeMessage:
            raise discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "cannot send")

    channel = ForbiddenChannel(CHANNEL, [])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    with pytest.raises(PermanentAdapterError, match="cannot send"):
        await adapter.send_message(CHANNEL, "x")
    client.connect_stopped.set()
    await adapter.close()
