from pydantic import BaseModel, ConfigDict


class FatalError(BaseModel):
    model_config = ConfigDict(frozen=True)

    reason: str
    restartable: bool
