from pydantic import BaseModel, ConfigDict

from src.domain.enum import BeamStatus


class Beam(BaseModel):
    model_config = ConfigDict(frozen=True)

    height: float | None
    raw_status: int | None
    status: tuple[BeamStatus, ...]
