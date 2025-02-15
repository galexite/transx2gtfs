from __future__ import annotations
from dataclasses import dataclass
import math
from multiprocessing import cpu_count
from pathlib import Path


@dataclass(frozen=True)
class Workload:
    input_files: list[Path | dict[str, Path]]
    file_size_limit: int
    gtfs_db: Path


def create_workers(
    input_files: list[Path | dict[str, Path]],
    worker_cnt: int | None = None,
    gtfs_db: Path | None = None,
    file_size_limit: int = 1000,
) -> list[Workload]:
    """Create workers for multiprocessing"""

    # Distribute the process into all cores
    if worker_cnt is not None and isinstance(worker_cnt, int):
        core_cnt = worker_cnt
    elif worker_cnt is None:
        if cpu_count() == 1:
            core_cnt = cpu_count()
        else:
            core_cnt = cpu_count() - 1
    else:
        assert isinstance(worker_cnt, int), (
            "The number of workers should be passed as an integer value."
        )

    # File count
    file_cnt = len(input_files)

    # Batch size
    batch_size = math.ceil(file_cnt / core_cnt)

    # Create journey workers
    workers: list[Workload] = []
    start_i = 0
    end_i = batch_size

    for i in range(0, core_cnt):
        # On the last iteration ensure that all the rest will be added
        if i == core_cnt - 1:
            # Slice the list
            selection = input_files[start_i:]
        else:
            # Slice the list
            selection = input_files[start_i:end_i]

        workers.append(
            Workload(
                input_files=selection, file_size_limit=file_size_limit, gtfs_db=gtfs_db
            )
        )

        # Update indices
        start_i += batch_size
        end_i += batch_size

    return workers
