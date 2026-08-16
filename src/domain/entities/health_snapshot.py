from pydantic import BaseModel, ConfigDict, Field

from src.domain.value_objects.types import Timestamp
from src.domain.value_objects.worker_health import WorkerHealth


class HealthSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts: Timestamp
    workers: dict[str, WorkerHealth]
    pending_count: int = Field(ge=0)
    quarantined_count: int = Field(ge=0)
    free_bytes: int = Field(ge=0)
    storage_paused: bool
    cpu_percent: float = Field(ge=0, le=100, allow_inf_nan=False)
    memory_percent: float = Field(ge=0, le=100, allow_inf_nan=False)
    disk_percent: float = Field(ge=0, le=100, allow_inf_nan=False)
