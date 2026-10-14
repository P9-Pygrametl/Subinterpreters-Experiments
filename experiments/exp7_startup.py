"""Exp 7: worker startup cost, isolated.

Measures only the cost of bringing workers up and tearing them down, with no
work in between:
  - subinterpreters: concurrent.interpreters.create() / close()
  - multiprocessing: fork Process start() / join()  (what pygrametl uses)
"""
import concurrent.interpreters as ci
import multiprocessing as mp
import os
import sys
import time
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics

REPEATS = 30
WORKER_COUNTS = [1, 2, 4, 8]
CTX = mp.get_context("fork")


def create_processes(workers: int) -> tuple[list[mp.Process], float]:
    processes = [CTX.Process() for _ in range(workers)]
    start = time.perf_counter()
    for process in processes:
        process.start()
    return processes, time.perf_counter() - start


def teardown_processes(processes: list[mp.Process]) -> tuple[float, bool]:
    start = time.perf_counter()
    for process in processes:
        process.join(60)

    elapsed = time.perf_counter() - start
    ok = all(process.exitcode == 0 for process in processes)

    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join()
        process.close()

    return elapsed, ok


def create_interpreters(workers: int) -> tuple[list[Any], float]:
    start = time.perf_counter()
    interpreters = [ci.create() for _ in range(workers)]
    return interpreters, time.perf_counter() - start


def teardown_interpreters(interpreters: list[Any]) -> tuple[float, bool]:
    start = time.perf_counter()
    for interpreter in interpreters:
        interpreter.close()
    return time.perf_counter() - start, True


MECHANISMS = {
    "fork": (create_processes, teardown_processes),
    "interpreters": (create_interpreters, teardown_interpreters),
}


def main() -> None:
    records = []
    for name, (create, teardown) in MECHANISMS.items():
        for workers in WORKER_COUNTS:
            create_times, teardown_times = [], []
            cpu_ms, mem_mb, all_ok = [], [], []
            for _ in range(REPEATS):
                cpu_before = metrics.tree_cpu_seconds()
                mem_before = metrics.tree_mem_mb()
                workers_objs, create_wall = create(workers)
                cpu_after = metrics.tree_cpu_seconds()
                mem_after = metrics.tree_mem_mb()
                teardown_wall, ok = teardown(workers_objs)
                create_times.append(create_wall)
                teardown_times.append(teardown_wall)
                cpu_ms.append((cpu_after - cpu_before) * 1000)
                mem_mb.append(max(0.0, mem_after - mem_before))
                all_ok.append(ok)
            records.append({
                "mechanism": name, "workers": workers,
                "create": metrics.summarize(create_times),
                "teardown": metrics.summarize(teardown_times),
                "cpu_ms": metrics.summarize(cpu_ms),
                "mem_delta_mb": metrics.summarize(mem_mb),
                "ok": all(all_ok),
            })

    rows = []
    for record in records:
        workers = record["workers"]
        create_median_ms = record["create"]["median"] * 1000
        rows.append([record["mechanism"], workers,
                     f"{create_median_ms:.2f}ms",
                     f"{create_median_ms / workers:.2f}ms",
                     f"{record['teardown']['median'] * 1000:.2f}ms",
                     f"{record['cpu_ms']['median']:.2f}ms",
                     f"{record['mem_delta_mb']['median']:.1f}MB",
                     record["ok"]])
    metrics.report(
        "exp7_startup", records, "Exp7 worker startup cost (isolated)",
        ["mechanism", "workers", "create", "per-worker", "teardown", "cpu",
         "mem delta", "ok"],
        rows, note=f"repeats={REPEATS} (median shown)")


if __name__ == "__main__":
    main()
