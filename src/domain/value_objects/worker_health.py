from pydantic import BaseModel, ConfigDict

from src.domain.value_objects.fatal_error import FatalError
from src.domain.value_objects.types import Timestamp


class WorkerHealth(BaseModel):
    """Срез одного воркера. Те же поля, что держит сам воркер."""

    model_config = ConfigDict(frozen=True)

    alive: bool
    last_error: str | None = None
    last_error_at: Timestamp | None = None
    fatal: FatalError | None = None
