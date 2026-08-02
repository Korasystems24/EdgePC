from enum import StrEnum


class RadarSourceType(StrEnum):
    MOCK = "mock"
    MODBUS = "modbus"
    WCF = "wcf"


class SourceState(StrEnum):
    HEALTHY = "HEALTHY"
    WCF_DOWN = "WCF_DOWN"
    WCF_RECOVERED = "WCF_RECOVERED"


class BeamStatus(StrEnum):
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
