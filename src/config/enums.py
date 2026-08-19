from enum import StrEnum


class RadarSourceType(StrEnum):
    MOCK = "mock"
    WCF = "wcf"


class NetworkGatewayType(StrEnum):
    MOCK = "mock"
    HTTP = "http"
