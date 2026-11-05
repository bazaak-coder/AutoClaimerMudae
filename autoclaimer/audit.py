"""Structured audit events (roadmap item 11 — P2).

A SEPARATE channel from debug logging:

- events are PERSISTED (``audit_events`` table, schema v7) and therefore
  useful after restart, queryable through the CLI ``audit`` command;
- emission is AUXILIARY: a failure to record an event is logged and
  swallowed — the audited operation never breaks;
- ``detail`` text passes the redaction filter at the emit site, so the
  trail can never contain credentials (belt-and-braces on top of the
  rule that emit sites pass secret-free text).

Categories: ``task`` (assigned/started/completed/failed/retried),
``account`` (enabled/disabled/paused/resumed/connected/disconnected/fatal
— the last three from gateway lifecycle wiring, ROADMAP item 26), ``runtime``
(paused/resumed), ``credential`` (set/deleted/imported/exported/rotated).
``configuration.changed`` is reserved for the runtime-reload feature
(roadmap item 13) — nothing changes configuration at runtime today.
"""

from __future__ import annotations

import logging
from typing import Protocol

from .logging_setup import redact_text

logger = logging.getLogger("autoclaimer.audit")


class _AuditSink(Protocol):
    async def record_audit_event(
        self,
        *,
        category: str,
        action: str,
        account_id: int | None = None,
        task_id: str | None = None,
        detail: str = "",
    ) -> None: ...


class AuditLog:
    """Emit-side facade over the store's audit table.

    ``emit`` never raises (auxiliary contract) and never persists
    secrets: *detail* is redacted here as defense in depth.
    """

    def __init__(self, sink: _AuditSink) -> None:
        self._sink = sink

    async def emit(
        self,
        *,
        category: str,
        action: str,
        account_id: int | None = None,
        task_id: str | None = None,
        detail: str = "",
    ) -> None:
        safe_detail = redact_text(str(detail))[:500]
        try:
            await self._sink.record_audit_event(
                category=category,
                action=action,
                account_id=account_id,
                task_id=task_id,
                detail=safe_detail,
            )
        except Exception:  # pragma: no cover - sink itself already contains
            logger.exception("audit emit failed")  # failures; double safety

        # Mirror to the dedicated audit logger (stdout/rotating file via
        # normal logging setup) — still separate from the debug logger.
        logger.info(
            "audit %s.%s",
            category,
            action,
            extra={
                "account_id": account_id,
                "task_id": task_id,
                "operation": f"audit.{category}.{action}",
                "outcome": safe_detail or "-",
            },
        )
