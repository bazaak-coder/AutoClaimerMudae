"""Retry safety for ambiguous Discord mutations (PR #2 review fix).

A 5xx response or an unrecognized library error during a MUTATING
operation does not prove the action was not applied — the rate limiter
must never blindly replay it (agents.md section 17.3: outcome-uncertain
rule). Read-only and pre-mutation failures keep their safe bounded
retries.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import discord
import pytest
from test_discord_adapter import (
    ACCOUNT,
    CHANNEL,
    FakeButton,
    FakeChannel,
    FakeClient,
    FakeMessage,
    make_adapter,
)

from autoclaimer.adapters import (
    PermanentAdapterError,
    RecoverableAdapterError,
    UncertainOutcomeError,
)
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import RateLimiter, RetryLimitExceededError, account_scope

RETRY_LIMIT = 3


def server_error() -> discord.DiscordServerError:
    return discord.DiscordServerError(
        SimpleNamespace(status=503, reason="unavailable", text="down"), "oops"
    )


async def run_through_limiter(fn: Any) -> Any:
    limiter = RateLimiter()
    return await limiter.run(fn, account_id=ACCOUNT, route="send_message")


def _advancing_sleep(clock: dict[str, datetime]) -> Any:
    async def sleep(seconds: float) -> None:
        clock["now"] = clock["now"] + timedelta(seconds=seconds)

    return sleep


# --- A: ambiguous mutations are never automatically repeated ---


async def test_send_5xx_is_uncertain_and_not_retried() -> None:
    """Send applied? Unknown (server errored after the request). Exactly
    one invocation; UncertainOutcomeError surfaces; no replay."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[str] = []

    async def failing_send(content: str) -> FakeMessage:
        attempts.append(content)
        raise server_error()

    channel.send = failing_send  # type: ignore[method-assign]
    adapter = make_adapter(client)

    with pytest.raises(UncertainOutcomeError, match="503"):
        await run_through_limiter(lambda: adapter.send_message(CHANNEL, "$wa"))
    assert attempts == ["$wa"]  # exactly one mutation attempt
    client.connect_stopped.set()
    await adapter.close()


async def test_reaction_5xx_is_uncertain_and_not_retried() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[int] = []

    async def failing_reaction(channel_id: int, message_id: int, emoji: str) -> None:
        attempts.append(message_id)
        raise server_error()

    client.http.add_reaction = failing_reaction  # type: ignore[method-assign]
    adapter = make_adapter(client)

    with pytest.raises(UncertainOutcomeError, match="503"):
        await run_through_limiter(lambda: adapter.add_reaction(CHANNEL, 7, "emoji"))
    assert attempts == [7]
    client.connect_stopped.set()
    await adapter.close()


async def test_component_click_5xx_is_uncertain_and_not_retried() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    button = FakeButton("claim", error=server_error())
    channel._messages.append(  # noqa: SLF001
        FakeMessage(9, "roll", components=[SimpleNamespace(children=[button])])
    )
    adapter = make_adapter(client)

    with pytest.raises(UncertainOutcomeError, match="503"):
        await run_through_limiter(lambda: adapter.press_component(CHANNEL, 9, "claim"))
    assert button.click_calls == 1  # exactly one mutation attempt
    client.connect_stopped.set()
    await adapter.close()


async def test_unknown_library_error_on_mutation_is_not_retried() -> None:
    """Insufficient evidence about a mutation's outcome must not become a
    safe-to-retry classification merely because the error is unexpected."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[str] = []

    async def failing_send(content: str) -> FakeMessage:
        attempts.append(content)
        raise discord.ClientException("mystery failure after send")

    channel.send = failing_send  # type: ignore[method-assign]
    adapter = make_adapter(client)

    with pytest.raises(UncertainOutcomeError, match="mystery"):
        await run_through_limiter(lambda: adapter.send_message(CHANNEL, "$wa"))
    assert attempts == ["$wa"]
    client.connect_stopped.set()
    await adapter.close()


async def test_click_connection_closed_after_send_is_uncertain_not_retried() -> None:
    """Reviewer scenario: the connection can fail after Discord received
    the interaction but before the client got confirmation. Non-fatal
    ConnectionClosed during a mutation → outcome unknown → exactly one
    click, never replayed."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    button = FakeButton("claim", error=discord.ConnectionClosed(1006, "abnormal closure"))
    channel._messages.append(  # noqa: SLF001
        FakeMessage(9, "roll", components=[SimpleNamespace(children=[button])])
    )
    adapter = make_adapter(client)

    with pytest.raises(UncertainOutcomeError, match="1006"):
        await run_through_limiter(lambda: adapter.press_component(CHANNEL, 9, "claim"))
    assert button.click_calls == 1
    client.connect_stopped.set()
    await adapter.close()


async def test_send_gateway_not_found_is_uncertain_not_retried() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[str] = []

    async def failing_send(content: str) -> FakeMessage:
        attempts.append(content)
        raise discord.GatewayNotFound()  # type: ignore[no-untyped-call]

    channel.send = failing_send  # type: ignore[method-assign]
    adapter = make_adapter(client)

    with pytest.raises(UncertainOutcomeError, match="gateway"):
        await run_through_limiter(lambda: adapter.send_message(CHANNEL, "$wa"))
    assert attempts == ["$wa"]
    client.connect_stopped.set()
    await adapter.close()


async def test_non_library_error_propagates_once() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[str] = []

    async def crashing_send(content: str) -> FakeMessage:
        attempts.append(content)
        raise RuntimeError("transport blew up")

    channel.send = crashing_send  # type: ignore[method-assign]
    adapter = make_adapter(client)

    with pytest.raises(RuntimeError, match="transport"):
        await run_through_limiter(lambda: adapter.send_message(CHANNEL, "$wa"))
    assert attempts == ["$wa"]
    client.connect_stopped.set()
    await adapter.close()


# --- B: safe transient retries remain functional ---


async def test_pre_mutation_failure_keeps_bounded_retry() -> None:
    """A failure classified as provably pre-side-effect (the adapter only
    raises RecoverableAdapterError before the library call once the fix
    lands; e.g. gateway-not-ready) keeps its safe bounded retry."""
    attempts: list[int] = []

    async def clean_transient() -> str:
        attempts.append(1)
        if len(attempts) == 1:
            raise RecoverableAdapterError("gateway not ready yet")
        return "applied"

    limiter = RateLimiter()
    result = await limiter.run(clean_transient, account_id=ACCOUNT, route="send_message")
    assert result == "applied"
    assert len(attempts) == 2  # retried exactly once, then succeeded


async def test_read_phase_5xx_is_recoverable_and_retried() -> None:
    """press_component's fetch (a READ) failing with 5xx is safe to
    retry; the click then happens exactly once."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    button = FakeButton("claim")
    message = FakeMessage(9, "roll", components=[SimpleNamespace(children=[button])])
    channel._messages.append(message)  # noqa: SLF001
    fetch_attempts: list[int] = []

    async def flaky_fetch(message_id: int) -> FakeMessage:
        fetch_attempts.append(message_id)
        if len(fetch_attempts) == 1:
            raise server_error()
        return message

    channel.fetch_message = flaky_fetch  # type: ignore[method-assign]
    adapter = make_adapter(client)

    result = await run_through_limiter(lambda: adapter.press_component(CHANNEL, 9, "claim"))
    assert result.acknowledged is True
    assert len(fetch_attempts) == 2  # read retried…
    assert button.click_calls == 1  # …mutation exactly once
    client.connect_stopped.set()
    await adapter.close()


async def test_read_phase_connection_closed_is_recoverable_and_retried() -> None:
    """Reads keep safe bounded retries for connection problems: a
    non-fatal ConnectionClosed during fetch_message proves no mutation
    happened."""
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    button = FakeButton("claim")
    message = FakeMessage(9, "roll", components=[SimpleNamespace(children=[button])])
    channel._messages.append(message)  # noqa: SLF001
    fetch_attempts: list[int] = []

    async def flaky_fetch(message_id: int) -> FakeMessage:
        fetch_attempts.append(message_id)
        if len(fetch_attempts) == 1:
            raise discord.ConnectionClosed(1006, "abnormal closure")
        return message

    channel.fetch_message = flaky_fetch  # type: ignore[method-assign]
    adapter = make_adapter(client)

    result = await run_through_limiter(lambda: adapter.press_component(CHANNEL, 9, "claim"))
    assert result.acknowledged is True
    assert len(fetch_attempts) == 2
    assert button.click_calls == 1
    client.connect_stopped.set()
    await adapter.close()


# --- C: 429 remains a recorded, respected cooldown ---


async def test_429_records_cooldown_persists_and_retries_safely(tmp_path: Any) -> None:
    """429 means the request was REJECTED, not applied — retry after the
    cooldown is safe even for mutations; the cooldown is recorded on the
    intended scopes and persisted. The limiter's clock advances with its
    fake sleep so the wait converges deterministically."""
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[str] = []

    async def throttled_send(content: str) -> FakeMessage:
        attempts.append(content)
        if len(attempts) == 1:
            raise discord.RateLimited(30.0)
        return FakeMessage(len(attempts), content)

    channel.send = throttled_send  # type: ignore[method-assign]
    adapter = make_adapter(client)

    clock = {"now": datetime.now(UTC)}
    limiter = RateLimiter(store=store, wall=lambda: clock["now"], sleep=_advancing_sleep(clock))
    sent = await limiter.run(
        lambda: adapter.send_message(CHANNEL, "$wa"),
        account_id=ACCOUNT,
        route="send_message",
    )
    assert sent.content == "$wa"
    assert attempts == ["$wa", "$wa"]  # rejected once, applied once
    active = await store.active_scope_cooldowns()
    assert account_scope(ACCOUNT) in active  # recorded + persisted
    client.connect_stopped.set()
    await adapter.close()
    await store.close()


# --- D: permanent failures are not retried ---


async def test_permanent_failure_invokes_once_and_propagates() -> None:
    channel = FakeChannel(CHANNEL, [])
    client = FakeClient(channel)
    attempts: list[str] = []

    async def forbidden_send(content: str) -> FakeMessage:
        attempts.append(content)
        raise discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "no")

    channel.send = forbidden_send  # type: ignore[method-assign]
    adapter = make_adapter(client)

    with pytest.raises(PermanentAdapterError, match="no"):
        await run_through_limiter(lambda: adapter.send_message(CHANNEL, "$wa"))
    assert attempts == ["$wa"]
    client.connect_stopped.set()
    await adapter.close()


# --- F: bounds, exhaustion, cancellation ---


async def test_safe_retry_exhaustion_is_bounded_with_context() -> None:
    attempts: list[int] = []

    async def always_recoverable() -> None:
        attempts.append(1)
        raise RecoverableAdapterError("clean pre-send failure")

    limiter = RateLimiter()
    with pytest.raises(RetryLimitExceededError, match="clean pre-send failure"):
        await limiter.run(always_recoverable, account_id=ACCOUNT, route="send_message")
    assert len(attempts) == RETRY_LIMIT + 1  # bounded: 1 + 3 retries


async def test_cancellation_propagates_through_run() -> None:
    started = asyncio.Event()

    async def hanging_operation() -> None:
        started.set()
        await asyncio.Event().wait()  # never completes

    limiter = RateLimiter()
    task = asyncio.create_task(
        limiter.run(hanging_operation, account_id=ACCOUNT, route="send_message")
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
