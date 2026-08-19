"""Совместимость таймаутов между собой — одним местом.

Числа из `.env` связаны иерархией, и нарушение любой ступени ломает надзор
тихо, поэтому проверки выполняются при загрузке настроек, а не в рантайме:

    таймаут транспорта < бюджет ретраев < дедлайн тика < heartbeat_max_silence
    restart_join_timeout < watchdog.inspect_timeout < heartbeat_max_silence
    restart_join_timeout < shutdown_timeout
    guard.check_interval < guard.threshold

Смысл ступеней: транспорт обязан вернуть управление раньше, чем истечёт тик;
тик обязан уложиться раньше, чем Watchdog решит, что воркер молчит; рестарт
обязан завершиться внутри одного inspect, иначе внешний таймаут отменит его до
эскалации.
"""

from src.config.budgets import SQLITE_SAVE_BUDGET_S, WCF_FIRST_TICK_HTTP_CALLS


def check_wcf_first_tick_fits_deadline(
    *, ingestion_tick_deadline: float, wcf_timeout: int
) -> None:
    """Первый тик дороже остальных: к обычному чтению добавляются разбор WSDL и
    обход каталога LIMACO."""
    budget = WCF_FIRST_TICK_HTTP_CALLS * wcf_timeout + SQLITE_SAVE_BUDGET_S
    if ingestion_tick_deadline <= budget:
        raise ValueError(
            f"ingestion.tick_deadline ({ingestion_tick_deadline}) должен быть > "
            f"бюджета первого тика WCF ({WCF_FIRST_TICK_HTTP_CALLS} "
            f"* wcf.timeout {wcf_timeout} + {SQLITE_SAVE_BUDGET_S}с на запись "
            f"= {budget})"
        )


def check_dispatch_budgets_fit_tick(
    *, tick_deadline: float, http_timeout: float, retrying_send_budget: float
) -> None:
    """За один тик dispatch последовательно шлёт health, события и телеметрию:
    health без ретраев, две остальные отправки — с ретраями."""
    if http_timeout >= tick_deadline:
        raise ValueError(
            f"HTTP-таймауты dispatch (максимум {http_timeout}) должны быть < "
            f"dispatch.tick_deadline ({tick_deadline})"
        )
    if retrying_send_budget >= tick_deadline:
        raise ValueError(
            f"бюджет ретраев dispatch ({retrying_send_budget}) должен быть < "
            f"dispatch.tick_deadline ({tick_deadline})"
        )
    sequential = http_timeout + 2 * retrying_send_budget
    if sequential > tick_deadline:
        raise ValueError(
            f"бюджет health+events+telemetry ({sequential}) должен быть <= "
            f"dispatch.tick_deadline ({tick_deadline})"
        )


def check_heartbeat_covers_ticks(
    *,
    heartbeat_max_silence: float,
    ingestion_tick_deadline: float,
    ingestion_interval: float,
    dispatch_heartbeat_budget: float,
) -> None:
    """Heartbeat ставится в начале тика, поэтому разрыв между отметками
    покрывает дедлайн тика, интервал ожидания и ретенцию после тика."""
    longest_gap = max(
        ingestion_tick_deadline,
        ingestion_interval,
        dispatch_heartbeat_budget,
    )
    if heartbeat_max_silence <= longest_gap:
        raise ValueError(
            f"heartbeat_max_silence ({heartbeat_max_silence}) должен быть > "
            f"самого долгого разрыва между heartbeat ({longest_gap})"
        )


def check_restart_join_fits_shutdown(
    *, restart_join_timeout: float, shutdown_timeout: float
) -> None:
    if restart_join_timeout >= shutdown_timeout:
        raise ValueError(
            f"restart_join_timeout ({restart_join_timeout}) должен быть < "
            f"shutdown_timeout ({shutdown_timeout})"
        )


def check_inspect_shorter_than_heartbeat(
    *, inspect_timeout: float, heartbeat_max_silence: float
) -> None:
    if inspect_timeout >= heartbeat_max_silence:
        raise ValueError(
            f"watchdog.inspect_timeout ({inspect_timeout}) должен быть < "
            f"heartbeat_max_silence ({heartbeat_max_silence})"
        )


def check_restart_join_fits_inspect(
    *, restart_join_timeout: float, inspect_timeout: float
) -> None:
    """Рестарт выполняется внутри одного inspect. Если join длиннее inspect,
    внешний timeout отменит рестарт до того, как WorkerRestartStuck успеет
    эскалировать отказ."""
    if restart_join_timeout >= inspect_timeout:
        raise ValueError(
            f"restart_join_timeout ({restart_join_timeout}) должен быть < "
            f"watchdog.inspect_timeout ({inspect_timeout})"
        )


def check_guard_interval_shorter_than_threshold(
    *, check_interval: float, threshold: float
) -> None:
    if check_interval >= threshold:
        raise ValueError(
            f"guard.check_interval ({check_interval}) должен быть < "
            f"guard.threshold ({threshold})"
        )


def check_storage_hysteresis(*, min_free_mb: int, resume_free_mb: int) -> None:
    """Без запаса на возобновление сбор будет включаться и выключаться на одном
    и том же значении свободного места."""
    if resume_free_mb <= min_free_mb:
        raise ValueError(
            f"ingestion.resume_free_mb ({resume_free_mb}) должен быть > "
            f"ingestion.min_free_mb ({min_free_mb})"
        )
