"""Rotating, redacting, structured logging setup.

Every log path — including formatted exception tracebacks — passes through
:func:`redact_text`, which scrubs credential-shaped values (Discord-style
tokens, webhook URLs, ``authorization``/``token``/``password``/``secret``
key-value pairs). Authentication tokens are never printed.

Structured context: log calls may attach ``account_id``, ``task_id``,
``operation``, ``outcome``, and ``scope`` via ``extra=...``; the formatter
renders them as ``key=value`` pairs after the message so every line carries
timestamps, severity, identifiers, operation category, and outcome.
"""

from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .models import LoggingSettings

LOGGER_NAME = "autoclaimer"

_REDACTED_WEBHOOK = "[REDACTED-WEBHOOK]"
_REDACTED_TOKEN = "[REDACTED-TOKEN]"
_REDACTED_VALUE = "[REDACTED]"

_WEBHOOK_RE = re.compile(r"https?://discord(?:app)?\.com/api/webhooks/\S+")
# Discord-style token shape: three dot-separated base64url segments
# (short user-ID segments included — the 20-char floor missed short IDs).
_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{6,10}\.[A-Za-z0-9_-]{20,}\b")
_KEY_VALUE_RE = re.compile(
    r"(?i)\b(authorization|token|password|passwd|secret|cookie|session"
    r"|session_id|api_key|apikey|client_secret|credential)\b['\"]?\s*[:=]\s*\S+"
)

_STRUCTURED_FIELDS = ("account_id", "task_id", "operation", "outcome", "scope")


def redact_text(text: str) -> str:
    """Scrub credential-shaped substrings from *text* (idempotent)."""
    text = _WEBHOOK_RE.sub(_REDACTED_WEBHOOK, text)
    text = _TOKEN_RE.sub(_REDACTED_TOKEN, text)

    def _redact_kv(match: re.Match[str]) -> str:
        key = match.group(0).split(":", 1)[0].split("=", 1)[0].rstrip(" '\"")
        return f"{key}={_REDACTED_VALUE}"

    return _KEY_VALUE_RE.sub(_redact_kv, text)


class StructuredRedactingFormatter(logging.Formatter):
    """Formats with structured context, then redacts the entire line."""

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = " ".join(
            f"{field}={record.__dict__[field]!r}"
            for field in _STRUCTURED_FIELDS
            if field in record.__dict__
        )
        line = f"{base} | {extras}" if extras else base
        return redact_text(line)


def setup_logging(
    settings: LoggingSettings, *, also_stderr: bool = True, logger_name: str = LOGGER_NAME
) -> logging.Logger:
    """Configure the package logger with a rotating, redacting file handler.

    Repeated calls replace existing handlers (idempotent for tests and
    reconfiguration). Returns the configured logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(settings.level.upper())
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = StructuredRedactingFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    log_path = Path(settings.log_file)
    if log_path.parent and not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=settings.max_bytes,
        backupCount=settings.backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if also_stderr:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    logger.propagate = False

    # Third-party library logs (discord.*) must pass the same redaction;
    # route them through this logger instead of the unredacted root.
    discord_logger = logging.getLogger("discord")
    discord_logger.handlers = list(logger.handlers)
    discord_logger.propagate = False
    return logger
