from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from src.domain.enum import BeamStatus


class Beam(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Высота может быть отрицательной (ниже опорной точки)?, но не NaN/Inf.
    height: Annotated[float, Field(allow_inf_nan=False)] | None
    raw_status: int | None
    status: tuple[BeamStatus, ...]
