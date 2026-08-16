from src.domain.enum import EdgeEventType
from src.domain.value_objects.event_payload import (
    PacketQuarantined,
    RadarDown,
    RadarRecovered,
    WorkerFatal,
    WorkerRestarted,
)
from src.domain.value_objects.types import EdgeEventPayload

BYTES_IN_MIB = 1024 * 1024

WCF_INIT_HTTP_CALLS = 1
WCF_CATALOG_HTTP_CALLS = 2
WCF_STEADY_HTTP_CALLS = 2
WCF_FIRST_TICK_HTTP_CALLS = (
    WCF_INIT_HTTP_CALLS + WCF_CATALOG_HTTP_CALLS + WCF_STEADY_HTTP_CALLS
)
SQLITE_SAVE_BUDGET_S = 2.0

PAYLOAD_BY_TYPE: dict[EdgeEventType, type[EdgeEventPayload]] = {
    EdgeEventType.RADAR_DOWN: RadarDown,
    EdgeEventType.RADAR_RECOVERED: RadarRecovered,
    EdgeEventType.PACKET_QUARANTINED: PacketQuarantined,
    EdgeEventType.WORKER_RESTARTED: WorkerRestarted,
    EdgeEventType.FATAL_ERROR: WorkerFatal,
}
