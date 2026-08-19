class RadarSourceError(Exception):
    """Базовое исключение для проблем с любым источником радарных данных."""


class RadarSourceUnavailable(RadarSourceError):
    """
    Источник временно недоступен: сеть, клиент не инициализирован,
    circuit breaker открыт. Стоит повторить попытку позже,
    сам процесс не критичен для остановки.
    """


class RadarSourceMisconfigured(RadarSourceError):
    """
    Источник настроен неверно (например, найдено 2 vessel вместо 1).
    Пустой каталог при старте LIMACO сюда не входит — это Unavailable.
    Само не пройдёт — нужно вмешательство человека в конфигурацию.
    """


class RadarSourceHung(RadarSourceError):
    """
    Блокирующее чтение источника не вернуло управление после дедлайна.
    Отмена await поток не останавливает; следующий тик поверх него нельзя.
    """


class RadarDataInvalid(RadarSourceError):
    """
    Источник ответил, данные получены, но не прошли валидацию
    (NaN, неизвестный статус, некорректная структура).
    Источник жив, просто конкретный пакет — мусор.
    """


class StorageError(Exception):
    """Не удалось прочитать или записать локальное хранилище (SQLite, диск)."""


class TelemetryStorageError(StorageError):
    """
    Пакет собран успешно, но не удалось сохранить его в хранилище.
    Источник ни при чём — проблема на нашей стороне (БД, диск).
    """


class NetworkError(Exception):
    """
    Базовое исключение для проблем с отправкой данных наружу.
    """


class NetworkUnavailable(NetworkError):
    """
    временно: сеть, timeout, 429, 5xx;
    """


class DispatchRejected(NetworkError):
    """
    сервер отверг данные с 422;
    """


class DispatchMisconfigured(NetworkError):
    """
    неверный токен или контракт: 400, 401, 403.
    """


class WorkerSupervisorError(Exception):
    """Сбой надзора за воркерами."""


class WorkerRestartStuck(WorkerSupervisorError):
    """
    stop/cancel не завершили задачу за отведённый timeout.
    Вторую задачу поверх живой создавать нельзя — иначе будет
    двойная отправка из dispatch.
    """


class WorkerAlreadyRunning(WorkerSupervisorError):
    """Попытка запустить воркер, пока предыдущая задача ещё жива."""


class ProcessMustExit(WorkerSupervisorError):
    """
    Watchdog эскалирует отказ на уровень процесса.
    Дальше внешний supervisor (Docker restart / Windows Service recovery).
    """
