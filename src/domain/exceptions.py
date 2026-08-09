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
    Само не пройдёт — нужно вмешательство человека в конфигурацию.
    """


class RadarDataInvalid(RadarSourceError):
    """
    Источник ответил, данные получены, но не прошли валидацию
    (NaN, неизвестный статус, некорректная структура).
    Источник жив, просто конкретный пакет — мусор.
    """


class TelemetryStorageError(Exception):
    """
    Пакет собран успешно, но не удалось сохранить его в хранилище.
    Источник ни при чём — проблема на нашей стороне (БД, диск).
    """


class NetworkSourseError(Exception):
    """
    Базовое исключение для проблем со всеми сетевыми данными.
    """


class NetworkUnavailable(NetworkSourseError):
    """
    временно: сеть, timeout, 429, 5xx;
    """


class DispatchRejected(NetworkSourseError):
    """
    сервер отверг данные с 422;
    """


class DispatchMisconfigured(NetworkSourseError):
    """
    неверный токен или контракт: 400, 401, 403.
    """
