"""Tests for the task type registry: registration, validation, execution."""

from collections.abc import Mapping
from typing import Any, cast

import pytest

from autoclaimer.adapters import AdapterError
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.registry import (
    DuplicateTaskTypeError,
    TaskConfigError,
    TaskContext,
    TaskOutcome,
    TaskRegistry,
    TaskRegistryError,
    TaskStatus,
    TaskType,
    UnknownTaskTypeError,
)


def accept_any_config(config: Mapping[str, Any]) -> None:
    """Validator that accepts every configuration."""


async def succeed_immediately(context: TaskContext) -> TaskOutcome:
    """Handler that never runs; used for lookup-only registration tests."""
    return TaskOutcome(TaskStatus.SUCCEEDED)


def require_text_config(config: Mapping[str, Any]) -> None:
    if not isinstance(config.get("text"), str):
        raise TaskConfigError("echo_task requires string config key 'text'.")


def make_echo_type(store: list[TaskContext] | None = None) -> TaskType:
    async def handler(context: TaskContext) -> TaskOutcome:
        if store is not None:
            store.append(context)
        return TaskOutcome(
            TaskStatus.SUCCEEDED, message="echoed", data={"echo": context.config["text"]}
        )

    return TaskType(
        id="echo_task",
        description="Echoes its 'text' config value.",
        validate_config=require_text_config,
        handler=handler,
    )


def make_context(task_id: str, config: Mapping[str, Any]) -> TaskContext:
    return TaskContext(
        account_id=271485504361996288, task_id=task_id, config=config, adapter=MockAdapter()
    )


# --- registration and lookup ---


def test_register_and_lookup_roundtrip() -> None:
    registry = TaskRegistry()
    task_type = make_echo_type()
    registry.register(task_type)
    assert registry.get("echo_task") is task_type
    assert "echo_task" in registry
    assert "other_task" not in registry
    assert registry.type_ids == ("echo_task",)


def test_duplicate_registration_is_rejected() -> None:
    registry = TaskRegistry()
    registry.register(make_echo_type())
    with pytest.raises(DuplicateTaskTypeError, match="'echo_task' is already registered"):
        registry.register(make_echo_type())


def test_unknown_task_type_is_rejected() -> None:
    with pytest.raises(UnknownTaskTypeError, match="Unknown task type 'missing_task'"):
        TaskRegistry().get("missing_task")


def test_type_ids_are_sorted() -> None:
    registry = TaskRegistry()
    for name in ("b_task", "a_task"):
        registry.register(
            TaskType(
                id=name,
                description="Stub type, handler never invoked.",
                validate_config=accept_any_config,
                handler=succeed_immediately,
            )
        )
    assert registry.type_ids == ("a_task", "b_task")


# --- task type definition validation ---


@pytest.mark.parametrize("bad_id", ["", "1bad", "has space", "x" * 65])
def test_task_type_requires_valid_id(bad_id: str) -> None:
    with pytest.raises(TaskRegistryError, match="id"):
        TaskType(
            id=bad_id,
            description="Stub.",
            validate_config=accept_any_config,
            handler=make_echo_type().handler,
        )


def test_task_type_requires_description() -> None:
    with pytest.raises(TaskRegistryError, match="description"):
        TaskType(
            id="ok_task",
            description="   ",
            validate_config=accept_any_config,
            handler=make_echo_type().handler,
        )


def test_task_type_requires_callable_members() -> None:
    with pytest.raises(TaskRegistryError, match="validator"):
        TaskType(
            id="ok_task",
            description="Stub.",
            validate_config=cast(Any, 123),
            handler=make_echo_type().handler,
        )
    with pytest.raises(TaskRegistryError, match="handler"):
        TaskType(
            id="ok_task",
            description="Stub.",
            validate_config=accept_any_config,
            handler=cast(Any, "not callable"),
        )


# --- configuration validation ---


def test_validate_config_accepts_valid_config() -> None:
    registry = TaskRegistry()
    registry.register(make_echo_type())
    registry.validate_config("echo_task", {"text": "hello"})


def test_validate_config_rejects_invalid_config() -> None:
    registry = TaskRegistry()
    registry.register(make_echo_type())
    with pytest.raises(TaskConfigError, match="string config key 'text'"):
        registry.validate_config("echo_task", {"text": 5})


def test_validate_config_rejects_unknown_type() -> None:
    with pytest.raises(UnknownTaskTypeError):
        TaskRegistry().validate_config("missing_task", {})


# --- asynchronous execution ---


async def test_execute_runs_handler_with_instance_context() -> None:
    seen: list[TaskContext] = []
    registry = TaskRegistry()
    registry.register(make_echo_type(seen))
    context = make_context("echo_task", {"text": "hello"})
    outcome = await registry.execute(context)
    assert outcome.status is TaskStatus.SUCCEEDED
    assert outcome.data == {"echo": "hello"}
    assert seen == [context]
    assert seen[0].account_id == 271485504361996288


async def test_execute_validates_config_before_running_handler() -> None:
    seen: list[TaskContext] = []
    registry = TaskRegistry()
    registry.register(make_echo_type(seen))
    with pytest.raises(TaskConfigError, match="text"):
        await registry.execute(make_context("echo_task", {"text": 5}))
    assert seen == []


async def test_execute_rejects_unknown_task_type() -> None:
    registry = TaskRegistry()
    registry.register(make_echo_type())
    with pytest.raises(UnknownTaskTypeError, match="missing_task"):
        await registry.execute(make_context("missing_task", {}))


async def test_handler_exceptions_propagate_unchanged() -> None:
    async def boom(context: TaskContext) -> TaskOutcome:
        raise AdapterError("channel disappeared")

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="boom_task",
            description="Always raises an adapter error.",
            validate_config=accept_any_config,
            handler=boom,
        )
    )
    with pytest.raises(AdapterError, match="channel disappeared"):
        await registry.execute(make_context("boom_task", {}))


async def test_recoverable_outcome_is_returned_not_raised() -> None:
    async def flaky(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="rate limited")

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="flaky_task",
            description="Returns a recoverable failure.",
            validate_config=accept_any_config,
            handler=flaky,
        )
    )
    outcome = await registry.execute(make_context("flaky_task", {}))
    assert outcome.status is TaskStatus.FAILED_RECOVERABLE
    assert outcome.message == "rate limited"


async def test_instances_are_independent_of_the_definition() -> None:
    seen: list[TaskContext] = []
    registry = TaskRegistry()
    registry.register(make_echo_type(seen))
    first = await registry.execute(make_context("echo_task", {"text": "one"}))
    second = await registry.execute(
        TaskContext(
            account_id=380028190142042112,
            task_id="echo_task",
            config={"text": "two"},
            adapter=MockAdapter(),
        )
    )
    assert first.data == {"echo": "one"}
    assert second.data == {"echo": "two"}
    assert len(seen) == 2
    assert seen[0].account_id != seen[1].account_id
