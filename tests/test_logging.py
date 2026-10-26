"""Tests for rotating, redacting, structured logging."""

import logging
from pathlib import Path

from autoclaimer.logging_setup import (
    LOGGER_NAME,
    StructuredRedactingFormatter,
    redact_text,
    setup_logging,
)
from autoclaimer.models import LoggingSettings

FAKE_TOKEN = "MTIzNDU2Nzg5MDEyMzQ1Njc4.GaBcDe.FgHiJkLmNoPqRsTuVwXyZ0123456789abcd"
FAKE_WEBHOOK = "https://discord.com/api/webhooks/1564289046732341311/supersecretvalue"


def test_redact_webhook_urls() -> None:
    assert redact_text(f"posting to {FAKE_WEBHOOK} now") == ("posting to [REDACTED-WEBHOOK] now")


def test_redact_discord_shaped_tokens() -> None:
    text = f"login failed for token {FAKE_TOKEN}"
    assert FAKE_TOKEN not in redact_text(text)
    assert "[REDACTED-TOKEN]" in redact_text(text)


def test_redact_credential_key_values() -> None:
    for line in (
        "token=abc123",
        "Authorization: Bearer xyz",
        'password = "hunter2"',
        "secret: s3cr3t",
        "api_key: 987654321",
    ):
        redacted = redact_text(line)
        assert "[REDACTED]" in redacted
        assert "abc123" not in redacted
        assert "Bearer xyz" not in redacted
        assert "hunter2" not in redacted
        assert "s3cr3t" not in redacted
        assert "987654321" not in redacted


def test_benign_text_survives_redaction() -> None:
    message = "task execution finished for instance 7 in 1.2 seconds"
    assert redact_text(message) == message


def test_redaction_is_idempotent() -> None:
    once = redact_text(f"token {FAKE_TOKEN} at {FAKE_WEBHOOK}")
    assert redact_text(once) == once


def make_record(message: str, level: int = logging.INFO, **extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="autoclaimer.test",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_formatter_renders_structured_context() -> None:
    formatter = StructuredRedactingFormatter("%(message)s")
    line = formatter.format(
        make_record(
            "task execution finished",
            account_id=271485504361996288,
            task_id="claim",
            operation="task.execute",
            outcome="succeeded",
        )
    )
    assert "account_id=271485504361996288" in line
    assert "task_id='claim'" in line
    assert "operation='task.execute'" in line
    assert "outcome='succeeded'" in line


def test_formatter_redacts_tracebacks_with_tokens() -> None:
    formatter = StructuredRedactingFormatter("%(message)s")
    try:
        raise RuntimeError(f"auth failed for {FAKE_TOKEN} via {FAKE_WEBHOOK}")
    except RuntimeError:
        import sys

        record = make_record("handler crashed", level=logging.ERROR)
        record.exc_info = sys.exc_info()
    line = formatter.format(record)
    assert FAKE_TOKEN not in line
    assert FAKE_WEBHOOK not in line
    assert "[REDACTED-TOKEN]" in line
    assert "[REDACTED-WEBHOOK]" in line


def test_setup_logging_is_idempotent(tmp_path: Path) -> None:
    settings = LoggingSettings(log_file=str(tmp_path / "a.log"))
    first = setup_logging(settings, also_stderr=False)
    handler_count = len(first.handlers)
    assert handler_count == 1
    second = setup_logging(settings, also_stderr=False)
    assert second is first
    assert len(second.handlers) == handler_count  # no duplicates


def test_rotating_file_respects_size_and_retention(tmp_path: Path) -> None:
    log_file = tmp_path / "rotate.log"
    settings = LoggingSettings(log_file=str(log_file), max_bytes=1024, backup_count=2, level="INFO")
    logger = setup_logging(settings, also_stderr=False)
    for index in range(60):
        logger.info("filler line %03d %s", index, "x" * 40)
    for handler in logger.handlers:
        handler.flush()
    assert log_file.exists()
    assert (tmp_path / "rotate.log.1").exists()
    assert (tmp_path / "rotate.log.2").exists()
    assert not (tmp_path / "rotate.log.3").exists()  # retention honored
    setup_logging(LoggingSettings(log_file=str(tmp_path / "cleanup.log")), also_stderr=False)
    del logger
    logging.getLogger(LOGGER_NAME).handlers.clear()
