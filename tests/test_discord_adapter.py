"""Unit tests for the discord.py-self adapter (fake client; no credentials)."""

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import discord
import pytest

from autoclaimer.adapters import (
    AdapterError,
    ChannelMessage,
    CooldownError,
    InteractionResult,
    PermanentAdapterError,
    RecoverableAdapterError,
    SentMessage,
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
    def __init__(self, channel_id: int, messages: list[FakeMessage]) -> None:
        self.id = channel_id
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

    async def history(self, *, limit: int = 100, **_: Any) -> AsyncIterator[FakeMessage]:
        # Newest first, like the real async generator.
        for message in reversed(self._messages[-limit:]):
            yield message


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


async def test_login_failure_is_permanent_and_token_never_leaks() -> None:
    client = FakeClient()

    async def fail_login(token: str) -> None:
        raise discord.LoginFailure("Improper token passed.")

    client.login = fail_login  # type: ignore[method-assign]
    adapter = make_adapter(client)
    with pytest.raises(PermanentAdapterError, match="Improper token"):
        await adapter.start()
    assert TOKEN not in repr(adapter)


# ----- pacing -----


async def test_random_delay_precedes_mutating_actions_not_reads() -> None:
    # _messages is chronological (oldest first): "a" then "b".
    messages = [FakeMessage(1, "a"), FakeMessage(2, "b")]
    channel = FakeChannel(CHANNEL, messages)
    client = FakeClient(channel)
    adapter = make_adapter(client)
    fetched = await adapter.fetch_recent_messages(CHANNEL)  # read: no jitter
    assert adapter.test_sleeps == []  # type: ignore[attr-defined]
    assert fetched == (
        ChannelMessage(
            id=1, channel_id=CHANNEL, author_id=111222333, content="a", has_components=False
        ),
        ChannelMessage(
            id=2, channel_id=CHANNEL, author_id=111222333, content="b", has_components=False
        ),
    )
    await adapter.send_message(CHANNEL, "hi")  # action: jitter applied
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


async def test_history_maps_components_flag() -> None:
    row = SimpleNamespace(children=[FakeButton("claim")])
    messages = [FakeMessage(1, "plain"), FakeMessage(2, "with buttons", components=[row])]
    channel = FakeChannel(CHANNEL, messages)
    client = FakeClient(channel)
    adapter = make_adapter(client)
    fetched = await adapter.fetch_recent_messages(CHANNEL)
    assert [m.has_components for m in fetched] == [False, True]
    client.connect_stopped.set()
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


async def test_button_click_without_response_is_recoverable_uncertain() -> None:
    button = FakeButton("claim", error=discord.InvalidData("no response received"))
    channel = FakeChannel(CHANNEL, [button_message(button, message_id=55)])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    with pytest.raises(RecoverableAdapterError, match="uncertain"):
        await adapter.press_component(CHANNEL, 55, "claim")
    client.connect_stopped.set()
    await adapter.close()


# ----- error translation -----


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
            PermanentAdapterError,
        ),
        (discord.ConnectionClosed(None), RecoverableAdapterError),
        (discord.GatewayNotFound(), RecoverableAdapterError),  # type: ignore[no-untyped-call]
    ]
    from autoclaimer.discord_adapter import _translate

    for exc, expected in cases:
        translated = _translate(exc)
        assert type(translated) is expected, f"{exc!r} -> {type(translated)}"


async def test_captcha_never_bypassed_message_is_explicit() -> None:
    from autoclaimer.discord_adapter import _translate

    exc = discord.CaptchaRequired(
        SimpleNamespace(status=428, reason="captcha"),
        {"captcha_key": ["required"], "captcha_sitekey": "site"},
    )
    translated = _translate(exc)
    assert isinstance(translated, PermanentAdapterError)
    assert "never bypassed" in str(translated)


async def test_connection_task_failure_surfaces_recoverably() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    adapter = make_adapter(client)
    await adapter.start()
    # Simulate the background connect task dying (callback stores the error).
    adapter._connect_error = discord.ConnectionClosed(None)  # noqa: SLF001
    with pytest.raises(RecoverableAdapterError, match="connection lost"):
        await adapter.send_message(CHANNEL, "x")
    client.connect_stopped.set()
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
