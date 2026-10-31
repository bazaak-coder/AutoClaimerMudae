"""Task type registry: definitions, validated configuration, async execution.

A *task type* (``TaskType``) is registered once and carries a stable
identifier, a human-readable description, a configuration validator, and an
asynchronous handler. A *task instance* is a per-account assignment; it is
represented at execution time by ``TaskContext`` (built from Phase 1's
``ResolvedTaskConfig``) and never stored here.

Task behaviour lives in the registered handlers — this module contains no
task-type conditionals. New task types are added by registering another
``TaskType``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from .adapters import PlatformAdapter
from .config import valid_task_id
from .credentials import CredentialProvider

ConfigValidator = Callable[[Mapping[str, Any]], None]
TaskHandler = Callable[["TaskContext"], Awaitable["TaskOutcome"]]


class TaskRegistryError(Exception):
    """Base class for task registry failures."""


class UnknownTaskTypeError(TaskRegistryError):
    """Raised when a task type id has not been registered."""


class DuplicateTaskTypeError(TaskRegistryError):
    """Raised when registering a task type id that already exists."""


class TaskConfigError(TaskRegistryError):
    """Raised when a task instance's configuration fails validation."""


class TaskStatus(Enum):
    """Terminal status of a task execution."""

    SUCCEEDED = "succeeded"
    FAILED_RECOVERABLE = "failed_recoverable"
    FAILED_PERMANENT = "failed_permanent"


@dataclass(frozen=True)
class TaskOutcome:
    """Structured result of one task execution.

    ``retry_at`` (milestone 4: retry ownership) lets a handler hand the
    scheduler a LEARNED absolute UTC deadline — parsed from a Mudae
    reply and anchored via ``autoclaimer.mudae.anchor_deadline`` —
    which supersedes generic transport backoff for recoverable
    outcomes. Attempt-count bounds still apply.
    """

    status: TaskStatus
    message: str = ""
    data: Mapping[str, Any] = field(default_factory=dict)
    retry_at: datetime | None = None


@dataclass(frozen=True)
class TaskContext:
    """Everything one execution of a task instance may need."""

    account_id: int
    task_id: str
    config: Mapping[str, Any]
    adapter: PlatformAdapter
    credentials: CredentialProvider | None = None


#: Logical channel roles a task may declare (resolved from the GLOBAL
#: channels configuration; see models.ChannelsConfig.effective()).
CHANNEL_ROLES = ("roll_claim", "mu", "rt")


@dataclass(frozen=True)
class TaskType:
    """Definition of one task type: identity, schema, and async behaviour."""

    id: str
    description: str
    validate_config: ConfigValidator
    handler: TaskHandler
    # Global channel role this task operates in (None: no channel). The
    # worker injects the resolved channel ID into the execution config;
    # handlers read it as config["channel"] and never resolve fallbacks.
    channel_role: str | None = None

    def __post_init__(self) -> None:
        if not valid_task_id(self.id):
            raise TaskRegistryError(
                f"Task type id {self.id!r} must start with a letter and contain only "
                "letters, digits, underscores, or hyphens (maximum 64 characters)."
            )
        if not self.description.strip():
            raise TaskRegistryError(f"Task type {self.id!r} needs a non-empty description.")
        if not callable(self.validate_config):
            raise TaskRegistryError(f"Task type {self.id!r} validator must be callable.")
        if not callable(self.handler):
            raise TaskRegistryError(f"Task type {self.id!r} handler must be callable.")
        if self.channel_role is not None and self.channel_role not in CHANNEL_ROLES:
            raise TaskRegistryError(
                f"Task type {self.id!r} channel_role must be one of "
                f"{', '.join(CHANNEL_ROLES)} (got {self.channel_role!r})."
            )


class TaskRegistry:
    """Registry of task types with validated, asynchronous execution."""

    def __init__(self) -> None:
        self._types: dict[str, TaskType] = {}

    def register(self, task_type: TaskType) -> None:
        """Register *task_type*; reject duplicate ids clearly."""
        if task_type.id in self._types:
            raise DuplicateTaskTypeError(f"Task type '{task_type.id}' is already registered.")
        self._types[task_type.id] = task_type

    def get(self, task_id: str) -> TaskType:
        """Return the registered type for *task_id*; reject unknown ids."""
        try:
            return self._types[task_id]
        except KeyError:
            raise UnknownTaskTypeError(f"Unknown task type '{task_id}'.") from None

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._types

    @property
    def type_ids(self) -> tuple[str, ...]:
        """All registered task type ids, sorted."""
        return tuple(sorted(self._types))

    def validate_config(self, task_id: str, config: Mapping[str, Any]) -> None:
        """Validate *config* against the schema of task type *task_id*."""
        self.get(task_id).validate_config(config)

    async def execute(self, context: TaskContext) -> TaskOutcome:
        """Validate the instance config, then run the type's handler.

        Adapter or handler exceptions propagate to the caller; retry and
        failure policies belong to later orchestration, not to the registry.
        """
        task_type = self.get(context.task_id)
        task_type.validate_config(context.config)
        return await task_type.handler(context)
