"""Exp 5: CPU-bound scaling with reused workers.

Workers are created once and reused; only steady-state work is timed. WALL /
CPU / MEM are recorded, raw runs kept, and the two mechanisms are interleaved
within each worker count.
"""

import multiprocessing as mp
import os
import random
import sys
from concurrent.futures import InterpreterPoolExecutor, ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics


ROW_COUNT = 600
WORK_PER_ROW = 60_000

STEADY_REPS = 10
SEQUENTIAL_REPS = 10

WORKER_COUNTS = [1, 2, 4, 8]
CHUNKSIZE = 10  # Batch tasks per message so IPC overhead is amortized

MP_CONTEXT = mp.get_context("fork")


def process_row(item: tuple[dict, int]) -> dict:
    """item = (row, work). Stateless so a subinterpreter can take it by code."""
    row, work = item
    checksum = 0

    for i in range(work):
        checksum = (checksum * 31 + i) % 1_000_003

    row["value"] = checksum
    return row


def make_items() -> list[tuple[dict, int]]:
    return [
        ({"i": i, "value": 0}, WORK_PER_ROW)
        for i in range(ROW_COUNT)
    ]


def make_pool(
    mechanism: str,
    worker_count: int,
) -> ProcessPoolExecutor | InterpreterPoolExecutor:
    if mechanism == "processes":
        return ProcessPoolExecutor(
            max_workers=worker_count,
            mp_context=MP_CONTEXT,
        )

    return InterpreterPoolExecutor(max_workers=worker_count)


def run_sequentially(items: list[tuple[dict, int]]) -> None:
    for item in items:
        process_row(item)


def run_parallel_steady(
    mechanism: str,
    worker_count: int,
    items: list[tuple[dict, int]],
) -> tuple[list[float], float, float, float, bool]:
    executor = make_pool(mechanism, worker_count)

    try:
        # Warm-up.
        list(executor.map(process_row, items, chunksize=CHUNKSIZE))

        output = list(executor.map(process_row, items, chunksize=CHUNKSIZE))
        expected = process_row(
            ({"i": -1, "value": 0}, WORK_PER_ROW)
        )["value"]

        ok = (
            len(output) == ROW_COUNT
            and all(row["value"] == expected for row in output)
        )

        mem_avg, mem_max = metrics.memory_over(
            lambda: list(
                executor.map(process_row, items, chunksize=CHUNKSIZE)
            )
        )

        wall_times = []
        cpu_total = 0.0

        for _ in range(STEADY_REPS):
            wall, cpu, _ = metrics.timed(
                lambda: list(
                    executor.map(process_row, items, chunksize=CHUNKSIZE)
                )
            )

            wall_times.append(wall)
            cpu_total += cpu
    finally:
        executor.shutdown()

    return wall_times, cpu_total, mem_avg, mem_max, ok


def run_sequential_steady(
    items: list[tuple[dict, int]],
) -> tuple[list[float], float, float, float]:
    # Warm-up.
    run_sequentially(items)

    mem_avg, mem_max = metrics.memory_over(
        lambda: run_sequentially(items)
    )

    wall_times = []
    cpu_total = 0.0

    for _ in range(SEQUENTIAL_REPS):
        wall, cpu, _ = metrics.timed(
            lambda: run_sequentially(items)
        )

        wall_times.append(wall)
        cpu_total += cpu

    return wall_times, cpu_total, mem_avg, mem_max


def main() -> None:
    records = []
    items = make_items()

    # Sequential baseline.
    wall_times, cpu_total, mem_avg, mem_max = run_sequential_steady(items)
    summary = metrics.summarize(wall_times)

    records.append({
        "mechanism": "sequential",
        "workers": 1,
        "steady": summary,
        "cpu": cpu_total,
        "mem_avg": mem_avg,
        "mem_max": mem_max,
        "ok": True,
    })

    randomizer = random.Random(1234)

    for worker_count in WORKER_COUNTS:
        order = ["processes", "interpreters"]
        randomizer.shuffle(order)

        for mechanism in order:
            (
                wall_times,
                cpu_total,
                mem_avg,
                mem_max,
                ok,
            ) = run_parallel_steady(mechanism, worker_count, items)

            summary = metrics.summarize(wall_times)

            records.append({
                "mechanism": mechanism,
                "workers": worker_count,
                "steady": summary,
                "cpu": cpu_total,
                "mem_avg": mem_avg,
                "mem_max": mem_max,
                "ok": ok,
            })

    sequential_median = records[0]["steady"]["median"]
    rows = []

    for record in records:
        summary = record["steady"]

        rows.append([
            record["mechanism"],
            record["workers"],
            f"{summary['median']:.3f}s",
            f"{summary['min']:.3f}..{summary['max']:.3f}",
            f"{sequential_median / summary['median']:.2f}x",
            f"{record['cpu']:.2f}s",
            f"{record['mem_avg']:.1f}MB",
            f"{record['mem_max']:.1f}MB",
            record["ok"],
        ])

    metrics.report(
        "exp5_cpu_memory",
        records,
        "Exp5 CPU scaling (workers reused)",
        [
            "mechanism",
            "workers",
            "steady",
            "min..max",
            "speedup",
            "cpu",
            "mem_avg",
            "mem_max",
            "ok",
        ],
        rows,
        note=(
            f"NROWS={ROW_COUNT} "
            f"WORK={WORK_PER_ROW} "
            f"steady_reps={STEADY_REPS}"
        ),
    )


if __name__ == "__main__":
    main()
