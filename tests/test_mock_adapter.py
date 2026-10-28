"""Tests for the deterministic mock adapter."""

import asyncio

import pytest

from autoclaimer.adapters import (
    ChannelMessage,
    CooldownError,
    InteractionResult,
    PermanentAdapterError,
    PlatformAdapter,
    RecoverableAdapterError,
    SentMessage,
)
from autoclaimer.mock_adapter import MockAdapter


def make_message(message_id: int, content: str = "roll") -> ChannelMessage:
    return ChannelMessage(
        id=message_id, channel_id=1, author_id=271485504361996288, content=content
    )


# --- deterministic defaults ---


async def test_default_send_message_is_successful_and_recorded() -> None:
    adapter = MockAdapter()
    first = await adapter.send_message(42, "hello")
    second = await adapter.send_message(42, "again")
    assert first.content == "hello"
    assert first.channel_id == 42
    assert second.message_id > first.message_id
    assert adapter.sent_messages == [first, second]
    assert [call.operation for call in adapter.calls] == ["send_message", "send_message"]
    assert adapter.calls[0].sequence == 1
    assert adapter.calls[0].arguments == {"channel_id": 42, "content": "hello"}


async def test_message_ids_are_deterministic_across_instances() -> None:
    first = await MockAdapter().send_message(1, "x")
    second = await MockAdapter().send_message(1, "x")
    assert first.message_id == second.message_id
    assert first.message_id == 900000000000000001


async def test_mock_emits_normalized_events_to_subscribers() -> None:
    from datetime import UTC, datetime

    from autoclaimer.adapters import MessageCreatedEvent

    adapter = MockAdapter()
    received: list[object] = []
    adapter.subscribe(received.append)
    await adapter.emit(
        MessageCreatedEvent(
            account_id=1,
            received_at=datetime.now(UTC),
            message=ChannelMessage(
                id=1, channel_id=2, author_id=3, content="roll", has_components=True
            ),
        )
    )
    await asyncio.sleep(0.01)  # pump delivery
    assert len(received) == 1
    await adapter.close()
    assert adapter.closed is True


async def test_press_component_default_is_acknowledged() -> None:
    adapter = MockAdapter()
    result = await adapter.press_component(7, 8, "claim_button")
    assert result == InteractionResult(message_id=8, component_id="claim_button", acknowledged=True)
    assert adapter.interactions == [result]


async def test_add_reaction_is_recorded() -> None:
    adapter = MockAdapter()
    await adapter.add_reaction(1, 2, "🎲")
    assert adapter.reactions == [(1, 2, "🎲")]


async def test_close_sets_state_and_is_recorded() -> None:
    adapter = MockAdapter()
    await adapter.close()
    assert adapter.closed is True
    assert adapter.calls[-1].operation == "close"


def test_mock_satisfies_platform_adapter_protocol() -> None:
    assert isinstance(MockAdapter(), PlatformAdapter)


# --- scripted successes ---


async def test_scripted_success_value_is_returned_fifo() -> None:
    adapter = MockAdapter()
    scripted = SentMessage(channel_id=9, message_id=555, content="scripted")
    adapter.script("send_message", scripted)
    assert await adapter.send_message(9, "real") == scripted
    assert adapter.sent_messages == [scripted]
    scripted_again = SentMessage(channel_id=9, message_id=556, content="second")
    adapter.script("send_message", scripted_again)
    assert await adapter.send_message(9, "real") == scripted_again


async def test_exhausted_script_falls_back_to_default() -> None:
    adapter = MockAdapter()
    adapter.script("send_message", SentMessage(channel_id=1, message_id=1, content="only"))
    await adapter.send_message(1, "first")
    fallback = await adapter.send_message(1, "second")
    assert fallback.content == "second"
    assert fallback.message_id > 1


# --- scripted failures and cooldowns ---


async def test_scripted_recoverable_failure_propagates() -> None:
    adapter = MockAdapter()
    adapter.script("send_message", RecoverableAdapterError("network blip"))
    with pytest.raises(RecoverableAdapterError, match="network blip"):
        await adapter.send_message(1, "x")
    assert adapter.sent_messages == []
    assert adapter.calls[0].operation == "send_message"


async def test_scripted_permanent_failure_propagates() -> None:
    adapter = MockAdapter()
    adapter.script("add_reaction", PermanentAdapterError("unknown emoji"))
    with pytest.raises(PermanentAdapterError, match="unknown emoji"):
        await adapter.add_reaction(1, 2, "🎲")
    assert adapter.reactions == []


async def test_cooldown_is_recoverable_and_carries_retry_after() -> None:
    adapter = MockAdapter()
    adapter.script("press_component", CooldownError(12.5))
    with pytest.raises(CooldownError) as excinfo:
        await adapter.press_component(1, 2, "claim_button")
    assert excinfo.value.retry_after_seconds == 12.5
    assert isinstance(excinfo.value, RecoverableAdapterError)
    assert adapter.interactions == []


async def test_scripted_interaction_result_is_returned() -> None:
    adapter = MockAdapter()
    result = InteractionResult(message_id=8, component_id="claim_button", acknowledged=False)
    adapter.script("press_component", result, PermanentAdapterError("component gone"))
    assert await adapter.press_component(7, 8, "claim_button") is result
    with pytest.raises(PermanentAdapterError, match="component gone"):
        await adapter.press_component(7, 8, "claim_button")
    assert adapter.interactions == [result]


# --- latency simulation ---


async def test_delay_applies_before_the_outcome() -> None:
    adapter = MockAdapter(delay_seconds=0.02)
    loop = asyncio.get_running_loop()
    started = loop.time()
    await adapter.send_message(1, "x")
    assert loop.time() - started >= 0.02


async def test_delay_applies_even_when_scripted_exception_fires() -> None:
    adapter = MockAdapter(delay_seconds=0.01)
    adapter.script("send_message", RecoverableAdapterError("late failure"))
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(RecoverableAdapterError):
        await adapter.send_message(1, "x")
    assert loop.time() - started >= 0.01


# --- channel discovery (cache-only, like the Discord adapter) ---


async def test_get_channel_and_validate_channel() -> None:
    from autoclaimer.adapters import ChannelInfo

    adapter = MockAdapter()
    adapter.add_channel(ChannelInfo(channel_id=7, name="general", channel_type="text"))
    assert await adapter.get_channel(7) == ChannelInfo(7, "general", "text")
    assert await adapter.validate_channel(7) is True
    assert await adapter.get_channel(8) is None
    assert await adapter.validate_channel(8) is False
    assert [call.operation for call in adapter.calls] == [
        "get_channel",
        "validate_channel",
        "get_channel",
        "validate_channel",
    ]
