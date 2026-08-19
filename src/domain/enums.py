from enum import StrEnum


class EdgeEventType(StrEnum):
    RADAR_DOWN = "radar_down"
    RADAR_RECOVERED = "radar_recovered"
    PACKET_QUARANTINED = "packet_quarantined"
    WORKER_RESTARTED = "worker_restarted"
    FATAL_ERROR = "fatal_error"


class BeamStatus(StrEnum):
    """Значения совпадают со строками WCF: они уже лежат в JSON-колонке beams,
    поэтому переименование сломает чтение сохранённых пакетов."""

    FAULT = "Fault"
    LENGTH_SIMULATED = "LengthSimulated"
    HEIGHT_SIMULATED = "HeightSimulated"
    TOO_LONG = "TooLong"
    UNKNOWN = "Unknown"
    TOO_SHORT = "TooShort"
    NOT_USED = "NotUsed"
    DISABLED = "Disabled"
    OUT_OF_PATTERN = "OutOfPattern"
    DISTANCE_NOT_UPDATING = "DistanceNotUpdating"
    WEAK_SIGNAL = "WeakSignal"
    OUT_OF_REPOSE_ANGLE = "OutOfReposeAngle"
    TOP_OUT_OF_REPOSE_ANGLE = "TopOutOfReposeAngle"
