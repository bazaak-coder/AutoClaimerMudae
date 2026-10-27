"""Batch 4 security regression tests (Phase 9 hardening)."""

from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import parse_app_config
from autoclaimer.logging_setup import redact_text
from autoclaimer.models import ConfigValidationError

ACCOUNT = 271485504361996288


def base_app() -> dict[str, Any]:
    return {"version": 1, "tasks": {"job": {"parameters": {}}}}


# D1: log_file traversal rejected


@pytest.mark.parametrize("bad", ["../evil.log", "a/../../evil.log", "..\\evil.log"])
def test_log_file_traversal_is_rejected(bad: str) -> None:
    raw = base_app()
    raw["logging"] = {"log_file": bad}
    with pytest.raises(ConfigValidationError, match="traverse"):
        parse_app_config(raw)


def test_log_file_absolute_and_relative_allowed() -> None:
    raw = base_app()
    raw["logging"] = {"log_file": "/var/log/autoclaimer.log"}
    assert parse_app_config(raw).logging.log_file == "/var/log/autoclaimer.log"
    raw["logging"] = {"log_file": "logs/ac.log"}
    assert parse_app_config(raw).logging.log_file == "logs/ac.log"


# D2: NaN/Infinity rejected in durations


@pytest.mark.parametrize(
    "inject",
    [
        {"min_interval_seconds": float("inf")},
        {"min_interval_seconds": float("nan")},
        {"backoff_base_seconds": 1e400},
    ],
)
def test_nonfinite_durations_are_rejected(inject: dict[str, float]) -> None:
    raw = base_app()
    raw["ratelimit"] = inject
    with pytest.raises(ConfigValidationError, match="finite"):
        parse_app_config(raw)


# D3: numeric ceilings enforced


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("settings", "max_concurrent_accounts", 65),
        ("settings", "shutdown_timeout_seconds", 601),
        ("logging", "max_bytes", 2_000_000_000),
        ("logging", "backup_count", 101),
        ("ratelimit", "retry_limit", 101),
    ],
)
def test_numeric_ceilings_are_enforced(section: str, key: str, value: int) -> None:
    raw = base_app()
    if section == "settings":
        raw["settings"] = {key: value}
    else:
        raw[section] = {key: value}
    with pytest.raises(ConfigValidationError, match="<="):
        parse_app_config(raw)


# D6: parameter size cap


def test_oversized_parameters_are_rejected() -> None:
    raw = base_app()
    raw["tasks"]["job"]["parameters"] = {"blob": "x" * 70_000}
    with pytest.raises(ConfigValidationError, match="64 KiB"):
        parse_app_config(raw)


# D5: redaction corpus extensions


def test_redaction_corpus_security() -> None:
    short_id_token = "MTIzNDU2Nzg5.GaBcDe.FgHiJkLmNoPqRsTuVwXyZ0123456789abcd"
    assert short_id_token not in redact_text(f"login as {short_id_token} ok")
    for line in (
        "cookie: sessionhash=abc",
        "client_secret=deadbeef",
        "passwd=hunter2",
        "session_id=xyz123",
    ):
        redacted = redact_text(line)
        assert "sessionhash=abc" not in redacted
        assert "deadbeef" not in redacted
        assert "hunter2" not in redacted
        assert "xyz123" not in redacted


def test_redaction_still_spares_benign_text() -> None:
    message = "account 123 executed job 456 in 1.5s outcome=succeeded"
    assert redact_text(message) == message


# D4: CLI survives store errors and always shuts down


async def test_cli_survives_store_error_and_shuts_down(tmp_path: Path) -> None:
    from autoclaimer.cli import AutoClaimerController, CliSession
    from autoclaimer.config import parse_accounts_config
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.persistence import DatabaseBusyError, TaskStateStore
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.registry import TaskRegistry, TaskType

    app = parse_app_config(base_app())
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()

    async def handler(context: Any) -> Any:
        return None

    registry.register(
        TaskType(
            id="job",
            description="probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )

    class BusyStore(TaskStateStore):
        async def list_instances(self, status: Any = None) -> tuple[Any, ...]:
            raise DatabaseBusyError("locked")

        async def close(self) -> None:
            if not getattr(self, "_closed_once", False):
                self._closed_once = True

    store = BusyStore(tmp_path / "unused.db")  # never opened; overridden methods
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=RateLimiter(),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
    )
    output: list[str] = []
    lines = iter(["tasks", "status"])

    async def read(prompt: str) -> str:
        try:
            return next(lines)
        except StopIteration:
            raise EOFError from None

    session = CliSession(controller, read_line=read, write=output.append)
    await session.run()  # must not raise despite DatabaseBusyError
    text = "\n".join(output)
    assert "DatabaseBusyError" in text  # error surfaced, REPL continued
    assert "shutdown complete" in text  # graceful shutdown still ran
    assert controller.closed is True
