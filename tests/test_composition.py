"""Deterministic composition-root tests (ROADMAP item 26).

Every test builds from real config FILES through ``build_runtime`` with
mocked externals only where physics demands it (the Fernet key comes
from an injected environ; no Discord login ever happens — adapters are
constructed lazily-login and nothing here starts workers).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from autoclaimer.credential_store import generate_fernet_key
from autoclaimer.discord_adapter import DiscordAdapter, LifecycleTransition
from autoclaimer.health import AccountHealthState, snapshot_for
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.runtime import RuntimeStartupError, build_runtime

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
CHANNEL = 111111111111111111

PRODUCTION_ENV = {"AUTOCLAIMER_FERNET_KEY": generate_fernet_key()}


def _write(path: Path, document: dict[str, object]) -> None:
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")


def _config_doc(tasks: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "version": 1,
        "channels": {"roll_claim": CHANNEL},
        "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
        "tasks": tasks
        if tasks is not None
        else {
            "roll": {"parameters": {"author_id": 123456789012345678, "username": "u"}},
            "mu": {"parameters": {"author_id": 123456789012345678, "username": "u"}},
            "rt": {"parameters": {"author_id": 123456789012345678}},
        },
    }


def _accounts_doc(
    entries: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "version": 1,
        "accounts": entries
        if entries is not None
        else [{"id": ACCOUNT, "tasks": ["roll", "mu", "rt"]}],
    }


def _write_files(
    tmp_path: Path,
    *,
    config: dict[str, object] | None = None,
    accounts: dict[str, object] | None = None,
) -> tuple[str, str]:
    _write(tmp_path / "config.json", config if config is not None else _config_doc())
    _write(
        tmp_path / "accounts.json",
        accounts if accounts is not None else _accounts_doc(),
    )
    return str(tmp_path / "config.json"), str(tmp_path / "accounts.json")


async def _store_token(tmp_path: Path, account_id: int, env: dict[str, str]) -> None:
    from autoclaimer.credential_store import SQLiteCredentialStore, load_fernet_keys

    store = SQLiteCredentialStore(tmp_path / "state.db", load_fernet_keys(env))
    await store.open()
    await store.set_token(account_id, f"fake-token-{account_id}")
    store.close()


# --- production construction ----------------------------------------------------------


async def test_production_build_registers_mudae_tasks_and_never_logs_in(
    tmp_path: Path,
) -> None:
    config, accounts = _write_files(tmp_path)
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)
    runtime = await build_runtime(config, accounts, environ=PRODUCTION_ENV)
    try:
        assert not runtime.demo
        assert {"roll", "mu", "rt"} <= set(runtime.registry.type_ids)
        adapter = runtime.adapter_factory(ACCOUNT)
        assert isinstance(adapter, DiscordAdapter)
        # Constructed, never started (lazy login): a fresh adapter reports
        # healthy-before-start and owns a client that has seen no login.
        assert adapter.is_healthy
        # Distinct isolated adapters per account; shared provider identity.
        other = runtime.adapter_factory(OTHER)
        assert isinstance(other, DiscordAdapter)
        assert other is not adapter
        assert other._credentials is adapter._credentials  # noqa: SLF001 — contract
    finally:
        await runtime.shutdown()


async def test_lifecycle_wiring_reaches_health_and_audit(tmp_path: Path) -> None:
    config, accounts = _write_files(tmp_path)
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)
    runtime = await build_runtime(config, accounts, environ=PRODUCTION_ENV)
    try:
        # Drive the composition's lifecycle hook exactly as the adapter will.
        adapter = runtime.adapter_factory(ACCOUNT)
        assert isinstance(adapter, DiscordAdapter)
        hook = adapter._on_lifecycle  # noqa: SLF001 — the wiring under test
        assert hook is not None
        result = hook(LifecycleTransition.CONNECTED)
        assert asyncio.iscoroutine(result)
        await result  # the audit emission the adapter would track and drain
        disconnect_audit = hook(LifecycleTransition.DISCONNECTED)
        assert asyncio.iscoroutine(disconnect_audit)
        await disconnect_audit
        fatal_audit = hook(LifecycleTransition.FATAL)
        assert asyncio.iscoroutine(fatal_audit)
        await fatal_audit
        snap = snapshot_for(runtime.health.snapshot(), ACCOUNT)
        assert snap.state is AccountHealthState.AUTH_FAILED  # fatal latched last
        rows = await runtime.store.audit_events(limit=10)
        actions = [row[2] for row in rows]  # (ts, category, action, ...)
        assert "connected" in actions and "disconnected" in actions and "fatal" in actions
    finally:
        await runtime.shutdown()


# --- fail-closed matrix ---------------------------------------------------------------


async def test_missing_fernet_key_fails_closed_and_leaks_nothing(tmp_path: Path) -> None:
    config, accounts = _write_files(tmp_path)
    with pytest.raises(RuntimeStartupError, match="AUTOCLAIMER_FERNET_KEY"):
        await build_runtime(config, accounts, environ={})
    # Nothing left open: the state DB is closed (a fresh store can open it).
    from autoclaimer.persistence import TaskStateStore

    probe = TaskStateStore(tmp_path / "state.db")
    await probe.open()
    await probe.close()


async def test_missing_token_for_enabled_account_fails_closed(tmp_path: Path) -> None:
    config, accounts = _write_files(
        tmp_path,
        accounts=_accounts_doc(
            [{"id": ACCOUNT, "tasks": ["roll"]}, {"id": OTHER, "tasks": ["rt"]}]
        ),
    )
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)  # OTHER has no token
    with pytest.raises(RuntimeStartupError, match=str(OTHER)) as excinfo:
        await build_runtime(config, accounts, environ=PRODUCTION_ENV)
    assert "credential set" in str(excinfo.value)


async def test_disabled_account_without_token_is_allowed(tmp_path: Path) -> None:
    config, accounts = _write_files(
        tmp_path,
        accounts=_accounts_doc(
            [{"id": ACCOUNT, "tasks": ["roll"]}, {"id": OTHER, "enabled": False, "tasks": ["rt"]}]
        ),
    )
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)
    runtime = await build_runtime(config, accounts, environ=PRODUCTION_ENV)
    await runtime.shutdown()  # disabled account needs no token


async def test_unregistered_task_assignment_fails_closed(tmp_path: Path) -> None:
    # "ghost" is DEFINED in config (so config validation passes) but no
    # task type registers it — the registry preflight must catch exactly
    # this divergence before any worker starts.
    config, accounts = _write_files(
        tmp_path,
        config=_config_doc(
            tasks={
                "roll": {"parameters": {"author_id": 123456789012345678, "username": "u"}},
                "ghost": {"parameters": {}},
            }
        ),
        accounts=_accounts_doc([{"id": ACCOUNT, "tasks": ["roll", "ghost"]}]),
    )
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)
    with pytest.raises(RuntimeStartupError, match="ghost"):
        await build_runtime(config, accounts, environ=PRODUCTION_ENV)


async def test_invalid_config_fails_closed(tmp_path: Path) -> None:
    _write(
        tmp_path / "config.json",
        {"version": 1, "settings": {}},  # channels missing entirely
    )
    _write(tmp_path / "accounts.json", _accounts_doc())
    with pytest.raises(Exception, match="channels"):
        await build_runtime(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json"), environ=PRODUCTION_ENV
        )
    assert not (tmp_path / "state.db").exists()  # nothing opened before validation


# --- demo posture ---------------------------------------------------------------------


async def test_demo_build_matches_the_historical_composition(tmp_path: Path) -> None:
    config, accounts = _write_files(
        tmp_path,
        config=_config_doc(tasks={"ping": {"parameters": {}}}),
        accounts=_accounts_doc([{"id": ACCOUNT, "tasks": ["ping"]}]),
    )
    # No Fernet key in the environment: the demo warns and continues.
    runtime = await build_runtime(config, accounts, demo=True, environ={})
    try:
        assert runtime.demo
        assert "ping" in runtime.registry.type_ids
        assert isinstance(runtime.adapter_factory(ACCOUNT), MockAdapter)
        assert runtime.credential_store is None
    finally:
        await runtime.shutdown()


# --- shutdown --------------------------------------------------------------------------


async def test_shutdown_is_idempotent_and_closes_stores(tmp_path: Path) -> None:
    from autoclaimer.persistence import TaskStateStore

    config, accounts = _write_files(tmp_path)
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)
    runtime = await build_runtime(config, accounts, environ=PRODUCTION_ENV)
    await runtime.shutdown()
    await runtime.shutdown()  # terminal one-shot: second call is a no-op
    assert runtime.controller.closed
    # The database reopens cleanly: nothing was left holding it.
    probe = TaskStateStore(tmp_path / "state.db")
    await probe.open()
    await probe.close()


async def test_cleanup_failure_does_not_mask_the_startup_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review finding 1: a credential-store close that RAISES during the
    abort cleanup must neither leak the state store nor replace the
    actionable RuntimeStartupError."""
    from autoclaimer.credential_store import SQLiteCredentialStore

    config, accounts = _write_files(tmp_path)
    await _store_token(tmp_path, ACCOUNT, PRODUCTION_ENV)

    def _exploding_close(store: SQLiteCredentialStore) -> None:
        # Patched onto the class (bound: the instance arrives as the first
        # argument): delegate to the real close first — no leaked
        # credential connection — THEN raise the injected error, the
        # containment path under test (review finding: the missing
        # parameter raised TypeError before the body ever ran).
        original_close(store)
        raise RuntimeError("close exploded (injected)")

    original_close = SQLiteCredentialStore.close
    calls: list[str] = []
    from autoclaimer.persistence import TaskStateStore

    real_store_close = TaskStateStore.close

    async def _recording_store_close(self: object) -> None:
        calls.append("store_closed")
        await real_store_close(self)  # type: ignore[arg-type]

    monkeypatch.setattr(SQLiteCredentialStore, "close", _exploding_close)
    monkeypatch.setattr(TaskStateStore, "close", _recording_store_close)
    try:
        # OTHER account has no token → abort AFTER the credential store opened.
        _write(
            tmp_path / "accounts.json",
            _accounts_doc([{"id": ACCOUNT, "tasks": ["roll"]}, {"id": OTHER, "tasks": ["rt"]}]),
        )
        with pytest.raises(RuntimeStartupError, match=str(OTHER)):
            await build_runtime(config, str(tmp_path / "accounts.json"), environ=PRODUCTION_ENV)
    finally:
        monkeypatch.setattr(SQLiteCredentialStore, "close", original_close)
    assert calls == ["store_closed"], (
        "the state store must still be closed when the credential-store "
        "close raises during abort cleanup"
    )
