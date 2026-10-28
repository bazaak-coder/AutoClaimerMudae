"""AutoClaimer's Mudae interface: input shapes, determinism, isolation."""

import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from autoclaimer.adapters import ChannelMessage, MessageCreatedEvent
from autoclaimer.mudae import InvalidMessageError, claim_status, parse_mudae
from autoclaimer.mudae_parser import MessageFamily

ACCOUNT = 271485504361996288

CLAIM_LINE = "**ExampleUser**, you can't claim for another **1h 42** min."


def channel_message(content: str) -> ChannelMessage:
    return ChannelMessage(
        id=1,
        channel_id=2,
        author_id=432,
        content=content,
        has_components=False,
        created_at=None,
    )


def created_event(content: str) -> MessageCreatedEvent:
    return MessageCreatedEvent(
        account_id=ACCOUNT,
        received_at=datetime.now(UTC),
        message=channel_message(content),
    )


# --- input shapes ---


def test_plain_string_parses() -> None:
    result = parse_mudae(CLAIM_LINE)
    assert claim_status(result) is not None


def test_channel_message_parses() -> None:
    result = parse_mudae(channel_message(CLAIM_LINE))
    status = claim_status(result)
    assert status is not None
    assert status.available is False
    assert status.reported == timedelta(hours=1, minutes=42)


def test_message_created_event_is_unwrapped() -> None:
    result = parse_mudae(created_event(CLAIM_LINE))
    assert claim_status(result) is not None
    assert result.account_username is None  # context only when not given


def test_account_username_is_echoed_not_extracted() -> None:
    result = parse_mudae(CLAIM_LINE, account_username="someone_else")
    assert result.account_username == "someone_else"
    status = claim_status(result)
    assert status is not None
    assert status.username == "ExampleUser"  # from the message, not the context


def test_nested_embed_shape_parses() -> None:
    # discord.py-self-style nested embed: roll card with claimable marker.
    message = SimpleNamespace(
        content="",
        embeds=[
            SimpleNamespace(
                description="Project SEKAI | Claims: #892 | **1,043**<:kakera:1>",
                footer=SimpleNamespace(text="React with any emoji to claim!"),
            )
        ],
    )
    result = parse_mudae(message)
    assert result.family is MessageFamily.CHARACTER


def test_flat_embed_dto_shape_parses_equivalently() -> None:
    # Flat DTO embed (LibzentSuite adapters.model.DiscordEmbed style).
    nested = SimpleNamespace(
        content="",
        embeds=[
            SimpleNamespace(
                description="Project SEKAI | Claims: #892 | **1,043**<:kakera:1>",
                footer=SimpleNamespace(text="React with any emoji to claim!"),
            )
        ],
    )
    flat = SimpleNamespace(
        content="",
        embeds=[
            SimpleNamespace(
                description="Project SEKAI | Claims: #892 | **1,043**<:kakera:1>",
                footer_text="React with any emoji to claim!",
            )
        ],
    )
    assert parse_mudae(flat).to_dict() == parse_mudae(nested).to_dict()


def test_missing_fields_do_not_raise() -> None:
    # A message with empty content and no embeds is simply unparseable
    # content: unknown messages never raise (upstream contract).
    result = parse_mudae(SimpleNamespace(content="", embeds=[], components=[]))
    assert claim_status(result) is None


def test_invalid_inputs_raise_typed_error() -> None:
    with pytest.raises(InvalidMessageError):
        parse_mudae(None)
    with pytest.raises(InvalidMessageError):
        parse_mudae(object())  # exposes no content/embeds/components


# --- determinism and boundaries ---


def test_parsing_is_deterministic() -> None:
    first = parse_mudae(CLAIM_LINE)
    second = parse_mudae(CLAIM_LINE)
    assert first.to_dict() == second.to_dict()


def test_durations_stay_relative_no_clock_or_deadline_at_parse() -> None:
    status = claim_status(parse_mudae(CLAIM_LINE))
    assert status is not None
    assert isinstance(status.reported, timedelta)
    # No absolute datetime anywhere in the claim view: anchoring to UTC
    # deadlines is the later Mudae task/state layer's job.
    assert not hasattr(status, "deadline")


# --- vendoring isolation ---


def test_no_libzentsuite_runtime_dependency() -> None:
    parse_mudae(CLAIM_LINE)  # ensure the parser is fully imported
    upstream_root = [
        name for name in sys.modules if name == "parsers" or name.startswith("parsers.")
    ]
    assert not upstream_root, f"leaked to upstream import root: {upstream_root}"
    assert not [name for name in sys.modules if "libzent" in name.lower()]
