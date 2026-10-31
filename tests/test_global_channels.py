"""Global channel configuration: parsing, centralized fallback, and the
end-to-end path from config.json through the worker into task handlers.

Every account must resolve to the SAME effective channels; optional mu/rt
fall back to roll_claim; per-task 'channel' parameters are rejected
(global guarantee, breaking migration).
"""

from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.models import ChannelsConfig, ConfigValidationError, ResolvedChannels
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288
OTHER = 380028190142042112

ROLL_CLAIM = 111111111111111111
MU_CHANNEL = 222222222222222222
RT_CHANNEL = 333333333333333333


# ----- model resolution -----


def test_effective_channels_fallback_semantics() -> None:
    only_roll = ChannelsConfig(roll_claim=ROLL_CLAIM)
    assert only_roll.effective() == ResolvedChannels(
        roll_claim=ROLL_CLAIM, mu=ROLL_CLAIM, rt=ROLL_CLAIM
    )
    explicit = ChannelsConfig(roll_claim=ROLL_CLAIM, mu=MU_CHANNEL, rt=None)
    resolved = explicit.effective()
    assert resolved.mu == MU_CHANNEL  # explicit respected
    assert resolved.rt == ROLL_CLAIM  # explicit None falls back
    mixed = ChannelsConfig(roll_claim=ROLL_CLAIM, mu=None, rt=RT_CHANNEL)
    assert mixed.effective().mu == ROLL_CLAIM
    assert mixed.effective().rt == RT_CHANNEL


def test_effective_channels_use_explicit_none_not_truthiness() -> None:
    # A future snowflake of 0 is invalid anyway; the resolution logic must
    # branch on `is None`, never on falsiness of the ID.
    cfg = ChannelsConfig(roll_claim=ROLL_CLAIM, mu=None, rt=RT_CHANNEL)
    assert cfg.effective().mu == ROLL_CLAIM


def test_resolved_channels_reject_unknown_roles() -> None:
    with pytest.raises(ValueError, match="unknown channel role"):
        ResolvedChannels(roll_claim=1, mu=2, rt=3).for_role("guild")


def test_channel_models_are_immutable() -> None:
    resolved = ChannelsConfig(roll_claim=ROLL_CLAIM).effective()
    with pytest.raises(AttributeError):
        resolved.mu = 1  # type: ignore[misc]


# ----- parsing -----


def parse(raw: dict[str, Any]) -> Any:
    return parse_app_config(raw)


def base_raw(channels: Any) -> dict[str, Any]:
    return {
        "version": 1,
        "channels": channels,
        "tasks": {"job": {"parameters": {}}},
    }


def test_all_three_channels_explicit() -> None:
    app = parse(base_raw({"roll_claim": ROLL_CLAIM, "mu": MU_CHANNEL, "rt": RT_CHANNEL}))
    assert app.channels.effective() == ResolvedChannels(ROLL_CLAIM, MU_CHANNEL, RT_CHANNEL)


@pytest.mark.parametrize(
    "channels",
    [
        {"roll_claim": ROLL_CLAIM},  # both omitted
        {"roll_claim": ROLL_CLAIM, "mu": None, "rt": None},  # both explicit null
        {"roll_claim": ROLL_CLAIM, "mu": None},  # mu null, rt omitted
        {"roll_claim": ROLL_CLAIM, "rt": None},  # rt null, mu omitted
    ],
)
def test_optional_channels_fall_back_to_roll_claim(channels: dict[str, Any]) -> None:
    app = parse(base_raw(channels))
    assert app.channels.effective() == ResolvedChannels(ROLL_CLAIM, ROLL_CLAIM, ROLL_CLAIM)


def test_mu_configured_rt_falls_back_and_vice_versa() -> None:
    app = parse(base_raw({"roll_claim": ROLL_CLAIM, "mu": MU_CHANNEL}))
    effective = app.channels.effective()
    assert effective.mu == MU_CHANNEL
    assert effective.rt == ROLL_CLAIM
    app = parse(base_raw({"roll_claim": ROLL_CLAIM, "rt": RT_CHANNEL}))
    effective = app.channels.effective()
    assert effective.mu == ROLL_CLAIM
    assert effective.rt == RT_CHANNEL


@pytest.mark.parametrize(
    "bad,where",
    [
        ({"mu": MU_CHANNEL, "rt": RT_CHANNEL}, "roll_claim"),  # missing mandatory
        ({"roll_claim": 0, "mu": MU_CHANNEL}, "roll_claim"),  # zero
        ({"roll_claim": -12}, "roll_claim"),  # negative
        ({"roll_claim": "123456789012345678"}, "roll_claim"),  # string
        ({"roll_claim": True}, "roll_claim"),  # bool (subclass of int!)
        ({"roll_claim": 123.0}, "roll_claim"),  # float
        ({"roll_claim": ROLL_CLAIM, "mu": True}, "mu"),  # bool optional
        ({"roll_claim": ROLL_CLAIM, "rt": "22"}, "rt"),  # string optional
        ({"roll_claim": ROLL_CLAIM, "rt": 0}, "rt"),  # zero optional
    ],
)
def test_invalid_channels_rejected_with_named_setting(bad: Any, where: str) -> None:
    with pytest.raises(ConfigValidationError, match=f"channels\\.{where}"):
        parse(base_raw(bad))


def test_unknown_channel_keys_rejected() -> None:
    with pytest.raises(ConfigValidationError, match="unknown key"):
        parse(base_raw({"roll_claim": ROLL_CLAIM, "general": 42}))


def test_missing_channels_section_is_actionable_error() -> None:
    with pytest.raises(ConfigValidationError, match="config.channels is required"):
        parse({"version": 1, "tasks": {"job": {"parameters": {}}}})


def test_task_parameter_channel_is_rejected_with_migration_hint() -> None:
    raw = base_raw({"roll_claim": ROLL_CLAIM})
    raw["tasks"]["job"]["parameters"]["channel"] = 42
    with pytest.raises(ConfigValidationError, match="may not set 'channel'"):
        parse(raw)


def test_account_override_channel_parameter_is_rejected() -> None:
    app = parse(base_raw({"roll_claim": ROLL_CLAIM}))
    accounts_raw = {
        "version": 1,
        "accounts": [
            {
                "id": ACCOUNT,
                "tasks": ["job"],
                "overrides": {"job": {"parameters": {"channel": 42}}},
            }
        ],
    }
    with pytest.raises(ConfigValidationError, match="may not set 'channel'"):
        parse_accounts_config(accounts_raw, app)


# ----- end to end: the channel each handler actually uses -----


async def wait_for_status(
    store: TaskStateStore, account_id: int, task_id: str, *statuses: TaskInstanceState
) -> None:
    import asyncio

    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(account_id, task_id)
        if record.status in statuses:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"instance {account_id}/{task_id} never reached {statuses}")


def build_harness(
    tmp_path: Path,
    *,
    channels: dict[str, Any],
) -> tuple[WorkerManager, TaskStateStore, dict[tuple[int, str], int]]:
    seen: dict[tuple[int, str], int] = {}

    async def handler(context: TaskContext) -> TaskOutcome:
        seen[(context.account_id, context.task_id)] = context.config["channel"]
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": channels,
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {
                "roll_probe": {"parameters": {}},
                "mu_probe": {"parameters": {}},
                "rt_probe": {"parameters": {}},
            },
        }
    )
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [
                {"id": ACCOUNT, "tasks": ["roll_probe", "mu_probe", "rt_probe"]},
                {"id": OTHER, "tasks": ["roll_probe", "mu_probe", "rt_probe"]},
            ],
        },
        app,
    )
    registry = TaskRegistry()
    for task_id, role in (
        ("roll_probe", "roll_claim"),
        ("mu_probe", "mu"),
        ("rt_probe", "rt"),
    ):
        registry.register(
            TaskType(
                id=task_id,
                description=f"Channel probe {task_id}.",
                validate_config=lambda config: None,
                handler=handler,
                channel_role=role,
            )
        )
    store = TaskStateStore(tmp_path / "state.db")
    return (
        WorkerManager(
            registry=registry,
            store=store,
            limiter=RateLimiter(store=store),
            app_config=app,
            accounts=accounts,
            adapter_factory=lambda account_id: MockAdapter(),
            idle_interval_seconds=0.01,
        ),
        store,
        seen,
    )


async def test_handlers_receive_the_correct_global_channel(tmp_path: Path) -> None:
    """roll -> roll_claim; mu -> mu; rt/rtu role -> rt — verified at the
    handler, through the full worker pipeline."""
    manager, store, seen = build_harness(
        tmp_path,
        channels={
            "roll_claim": ROLL_CLAIM,
            "mu": MU_CHANNEL,
            "rt": RT_CHANNEL,
        },
    )
    await store.open()
    async with manager:
        for account in (ACCOUNT, OTHER):
            for task_id in ("roll_probe", "mu_probe", "rt_probe"):
                await wait_for_status(store, account, task_id, TaskInstanceState.SUCCEEDED)
    assert seen == {
        (ACCOUNT, "roll_probe"): ROLL_CLAIM,
        (ACCOUNT, "mu_probe"): MU_CHANNEL,
        (ACCOUNT, "rt_probe"): RT_CHANNEL,
        (OTHER, "roll_probe"): ROLL_CLAIM,
        (OTHER, "mu_probe"): MU_CHANNEL,
        (OTHER, "rt_probe"): RT_CHANNEL,
    }
    await store.close()


async def test_every_account_resolves_identical_effective_channels(tmp_path: Path) -> None:
    """Fallback resolved centrally: with only roll_claim configured, mu/rt
    handlers see roll_claim on BOTH accounts."""
    manager, store, seen = build_harness(tmp_path, channels={"roll_claim": ROLL_CLAIM})
    await store.open()
    async with manager:
        for account in (ACCOUNT, OTHER):
            for task_id in ("roll_probe", "mu_probe", "rt_probe"):
                await wait_for_status(store, account, task_id, TaskInstanceState.SUCCEEDED)
    assert set(seen.values()) == {ROLL_CLAIM}
    assert len(seen) == 6  # both accounts, all three tasks
    await store.close()


async def test_partial_dedicated_channels_hit_the_right_tasks(tmp_path: Path) -> None:
    manager, store, seen = build_harness(
        tmp_path, channels={"roll_claim": ROLL_CLAIM, "mu": MU_CHANNEL}
    )
    await store.open()
    async with manager:
        for account in (ACCOUNT, OTHER):
            for task_id in ("roll_probe", "mu_probe", "rt_probe"):
                await wait_for_status(store, account, task_id, TaskInstanceState.SUCCEEDED)
    assert seen[(ACCOUNT, "mu_probe")] == MU_CHANNEL  # dedicated
    assert seen[(ACCOUNT, "rt_probe")] == ROLL_CLAIM  # fallback
    assert seen[(ACCOUNT, "roll_probe")] == ROLL_CLAIM
    await store.close()


def test_mudae_task_registrations_declare_the_right_roles() -> None:
    """The real Mudae task types map to the global channel roles:
    roll(+claims) -> roll_claim, mu -> mu, rt($rt and $rtu) -> rt."""
    from autoclaimer.mudae_tasks import register_mudae_tasks
    from autoclaimer.persistence import TaskStateStore

    class _NullStore(TaskStateStore):
        def __init__(self) -> None:  # registration needs no live store
            pass

    registry = TaskRegistry()
    register_mudae_tasks(registry, store=_NullStore())
    roles = {task_id: registry.get(task_id).channel_role for task_id in ("roll", "mu", "rt")}
    assert roles == {"roll": "roll_claim", "mu": "mu", "rt": "rt"}
