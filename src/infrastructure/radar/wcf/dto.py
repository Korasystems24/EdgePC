import math
from typing import Self, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.domain.enum import BeamStatus
from src.infrastructure.radar.wcf.exceptions import ValueRadarError


def _unwrap_array(value: object, item_key: str) -> object:
    if isinstance(value, dict):
        typed_value = cast(dict[str, object], value)
        return typed_value.get(item_key, [])
    if hasattr(value, item_key):
        return getattr(value, item_key) or []
    return value


class NonFiniteRejectingModel(BaseModel):
    @field_validator("*", mode="after")
    @classmethod
    def _reject_non_finite(cls, value: object) -> object:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueRadarError("Значение должно быть конечным числом (не NaN/Inf)")
        return value


class WCFDataDTO(NonFiniteRejectingModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        from_attributes=True,
    )
    mass: float | None = Field(default=None, alias="mass")
    density: float | None = Field(default=None, alias="Density")
    height: float | None = Field(default=None, alias="height")
    diameter: float | None = Field(default=None, alias="diameter")
    volume: float | None = Field(
        default=None, alias="volume"
    )  # TODO: подтвердить, что ключ реально приходит

    @classmethod
    def from_raw(cls, raw_data: str) -> Self:
        extracted = {
            parts[1]: parts[2].strip()
            for line in raw_data.splitlines()
            if len(parts := line.split(":", 2)) == 3
        }
        return cls.model_validate(extracted)


class WCFBeamDTO(NonFiniteRejectingModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        from_attributes=True,
    )

    end_point_height: float | None = Field(default=None, alias="BeamEndPointHeight")
    status: list[BeamStatus] | None = Field(default=None, alias="BeamStatus")
    name: str | None = Field(default=None, alias="Name")
    raw_status: int | None = Field(default=None, alias="RawStatus")


class WCFSensorDTO(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        from_attributes=True,
    )

    beams: list[WCFBeamDTO] = Field(default_factory=list[WCFBeamDTO], alias="Beams")

    @field_validator("beams", mode="before")
    @classmethod
    def _unwrap_beams(cls, value: object) -> object:
        return _unwrap_array(value, "BeamCalculatedData")


class WCFVesselDTO(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        from_attributes=True,
    )

    average_level: float | None = Field(default=None, alias="AverageLevel")
    sensors: list[WCFSensorDTO] = Field(
        default_factory=list[WCFSensorDTO], alias="Sensors"
    )

    @field_validator("sensors", mode="before")
    @classmethod
    def _unwrap_sensors(cls, value: object) -> object:
        return _unwrap_array(value, "SensorCalculatedData")

    @classmethod
    def from_zeep(cls, raw_vessel: object) -> Self:
        return cls.model_validate(raw_vessel)

    def first_sensor_beams(self) -> list[WCFBeamDTO]:
        if not self.sensors:
            return []
        return self.sensors[0].beams
