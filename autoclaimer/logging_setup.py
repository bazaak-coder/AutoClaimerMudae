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
# Discord-style token shape: three dot-separated base64url segments.
# Wide windows (audit fix): the middle cap {6,10} let 11+ char middle
# segments escape entirely; short first segments (small snowflakes) and
# 19-char third segments likewise — all widened with only a lower bound.
_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\b")
# Key-value credential matcher (audit fix): the value side consumes the
# REST OF THE LINE — "Authorization: Bearer <secret>" previously
# redacted only the word "Bearer", leaking the secret itself; and bare
# "Bearer <secret>" lines (no separator) are redacted whole.
_KEY_VALUE_RE = re.compile(
    r"(?i)\b(authorization|token|password|passwd|secret|cookie|session"
    r"|session_id|api_key|apikey|api-key|x-api-key|access_token|refresh_token"
    r"|bot_token|discord_token|client_secret|private_key|credential"
    r"|auth)\b['\"]?\s*[:=]\s*[^\n]*"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+\S+")

_STRUCTURED_FIELDS = ("account_id", "task_id", "operation", "outcome", "scope")


def redact_text(text: str) -> str:
    """Scrub credential-shaped substrings from *text* (idempotent)."""
    text = _WEBHOOK_RE.sub(_REDACTED_WEBHOOK, text)
    text = _TOKEN_RE.sub(_REDACTED_TOKEN, text)

    def _redact_kv(match: re.Match[str]) -> str:
        key = match.group(0).split(":", 1)[0].split("=", 1)[0].rstrip(" '\"")
        return f"{key}={_REDACTED_VALUE}"

    text = _KEY_VALUE_RE.sub(_redact_kv, text)
    return _BEARER_RE.sub(lambda m: m.group(0).split()[0] + " " + _REDACTED_VALUE, text)


class _RedactingRotatingFileHandler(RotatingFileHandler):
    """Rotation failures must never dump raw records to stderr.

    CPython's ``Handler.handleError`` prints ``Message: {record.msg!r}``
    and ``Arguments: {record.args}`` UNREDACTED (audit fix: a full disk
    leaked every subsequent record raw to stderr). Redact the payload
    before delegating.
    """

    def handleError(self, record: logging.LogRecord) -> None:
        try:
            # Replace the raw pieces CPython prints with the fully
            # redacted FORMATTED line — bare args (an opaque secret with
            # no recognizable shape) would otherwise survive piecewise
            # redaction (verified by the regression test).
            record.msg = self.format(record)
            record.args = None
        except Exception:  # pragma: no cover - formatting already failed
            record.msg = redact_text(str(record.msg))
            record.args = None
        super().handleError(record)


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

    Review finding (reload hardening): the swap is ATOMIC — the new file
    handler is constructed (the failure point: unwritable path, missing
    permissions) BEFORE any old handler is removed, so a failed
    reconfiguration leaves the previous, working logging in place.
    """
    logger = logging.getLogger(logger_name)

    formatter = StructuredRedactingFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    # Construct FIRST: an OSError here must not have removed anything —
    # and the level is only applied once the new configuration is
    # certain to take effect (a failed reload used to keep the old
    # handlers but apply the NEW level, silently filtering records).
    log_path = Path(settings.log_file)
    if log_path.parent and not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = _RedactingRotatingFileHandler(
        log_path,
        maxBytes=settings.max_bytes,
        backupCount=settings.backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.setLevel(settings.level.upper())

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.addHandler(file_handler)

    if also_stderr:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    logger.propagate = False

    # Third-party library logs (discord.*) must pass the same redaction;
    # route them through this logger instead of the unredacted root.
    # Leveled too (audit fix): without setLevel the effective level came
    # from the unconfigured root (WARNING), silently dropping discord
    # INFO/DEBUG regardless of settings.level.
    discord_logger = logging.getLogger("discord")
    discord_logger.handlers = list(logger.handlers)
    discord_logger.setLevel(settings.level.upper())
    discord_logger.propagate = False
    return logger
