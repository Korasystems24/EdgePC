import pytest
from pydantic import ValidationError

from src.config.settings import (
    DispatchSettings,
    IngestionSettings,
    Settings,
    WatchdogSettings,
    WcfSettings,
)
from src.domain.constants import SQLITE_SAVE_BUDGET_S, WCF_FIRST_TICK_HTTP_CALLS


def test_default_settings_are_valid() -> None:
    settings = Settings()
    budget = WCF_FIRST_TICK_HTTP_CALLS * settings.wcf.timeout + SQLITE_SAVE_BUDGET_S
    assert settings.ingestion.tick_deadline > budget
    sequential = (
        settings.dispatch.http_timeout + 2 * settings.dispatch.retrying_send_budget
    )
    assert sequential <= settings.dispatch.tick_deadline


def test_wcf_first_tick_budget_must_fit_ingestion_deadline() -> None:
    with pytest.raises(ValidationError, match="WCF first-tick budget"):
        Settings(
            wcf=WcfSettings(timeout=3),
            ingestion=IngestionSettings(tick_deadline=8.0),
        )


def test_wcf_first_tick_budget_accepts_room_for_calls_and_save() -> None:
    settings = Settings(
        wcf=WcfSettings(timeout=3),
        ingestion=IngestionSettings(tick_deadline=20.0),
    )
    budget = WCF_FIRST_TICK_HTTP_CALLS * 3 + SQLITE_SAVE_BUDGET_S
    assert settings.ingestion.tick_deadline > budget


def test_dispatch_retry_budget_must_fit_tick() -> None:
    with pytest.raises(ValidationError, match="health\\+events\\+telemetry budget"):
        DispatchSettings(
            tick_deadline=45.0,
            retry_attempts=3,
            retry_wait_max=4.0,
            connect_timeout=5.0,
            read_timeout=5.0,
            write_timeout=5.0,
            pool_timeout=5.0,
        )


def test_inspect_timeout_must_be_shorter_than_heartbeat() -> None:
    with pytest.raises(ValidationError, match="inspect_timeout"):
        Settings(
            heartbeat_max_silence=60.0,
            watchdog=WatchdogSettings(inspect_timeout=60.0),
        )
