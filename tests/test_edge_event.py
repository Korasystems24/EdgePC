import uuid

import pytest
from pydantic import ValidationError

from src.domain.entities.edge_event import EdgeEvent
from src.domain.enum import EdgeEventType
from src.domain.value_objects.event_payload import RadarDown, RadarRecovered


def test_factory_binds_type_to_payload() -> None:
    event = EdgeEvent.radar_down(reason="WCF timeout")
    assert event.type is EdgeEventType.RADAR_DOWN
    assert event.payload == RadarDown(reason="WCF timeout")


def test_rejects_mismatched_payload() -> None:
    with pytest.raises(ValidationError, match="radar_down"):
        EdgeEvent(
            type=EdgeEventType.RADAR_DOWN,
            payload=RadarRecovered(downtime_seconds=1.0),
        )


def test_quarantine_keeps_packet_event_id_as_uuid() -> None:
    packet_event_id = uuid.uuid4()
    event = EdgeEvent.packet_quarantined(
        telemetry_packet_id=7,
        telemetry_packet_event_id=packet_event_id,
        attempts=3,
    )
    assert event.payload.telemetry_packet_event_id == packet_event_id
