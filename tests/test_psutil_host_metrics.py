from pathlib import Path

import pytest

from src.domain.exceptions import StorageError
from src.infrastructure.metrics.psutil_host_metrics import PsutilHostMetrics


def test_sample_returns_finite_percents(tmp_path: Path) -> None:
    metrics = PsutilHostMetrics(tmp_path)
    sample = metrics.sample()
    for value in (sample.cpu_percent, sample.memory_percent, sample.disk_percent):
        assert 0.0 <= value <= 100.0


def test_missing_disk_path_raises_storage_error(tmp_path: Path) -> None:
    missing = tmp_path / "no-such-dir"
    metrics = PsutilHostMetrics(missing)
    with pytest.raises(StorageError, match="диска"):
        metrics.sample()
