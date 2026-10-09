"""Shared measurement metrics and helpers for the subinterpreter vs
multiprocessing study.

Provides:
  - process-tree CPU time (summed across all worker processes, in seconds)
  - sampled process-tree memory (average and peak, in MB)
  - ResourceMonitor + summarize() for timing and aggregation
  - report() to emit an experiment's table and records

Uses psutil for CPU/memory of the process tree (no /proc parsing).
"""
import json
import os
import statistics
import sys
import threading
import time
from collections.abc import Sequence
from typing import Any, Protocol, TypedDict

import psutil

MB = 1024.0 * 1024.0


Summary = TypedDict("Summary", {
    "median": float,
    "min": float,
    "max": float,
    "n": int,
    "runs": list[float],
})


class Queue(Protocol):
    """The queue interface the consumers rely on. Both multiprocessing queues
    and concurrent.interpreters queues satisfy it."""
    def get(self, *args: Any) -> Any: ...
    def put(self, _: Any, *args: Any) -> Any: ...


# --------------------------------------------------------------------------
# Process tree CPU + memory
# --------------------------------------------------------------------------
def _tree(root: int) -> list[psutil.Process]:
    try:
        proc = psutil.Process(root)
        return [proc] + proc.children(recursive=True)
    except psutil.Error:
        return []


def _cpu_seconds(proc: psutil.Process) -> float | None:
    try:
        t = proc.cpu_times()
        return t.user + t.system
    except psutil.Error:
        return None


def _mem_mb(proc: psutil.Process) -> float:
    try:
        info = proc.memory_full_info()
        value = getattr(info, "pss", None) or info.rss
    except psutil.Error:
        try:
            value = proc.memory_info().rss
        except psutil.Error:
            return 0.0
    return value / MB


def tree_cpu_seconds(root: int = 0) -> float:
    root = root or os.getpid()
    return sum(c for c in (_cpu_seconds(p) for p in _tree(root)) if c)


def tree_mem_mb(root: int = 0) -> float:
    root = root or os.getpid()
    return sum(_mem_mb(p) for p in _tree(root))


def timed(fn):
    """Run fn() with nothing else running.

    Returns (wall_seconds, tree_cpu_seconds, result). The process-tree CPU is a
    start/stop delta, so no sampling thread is needed and nothing competes with
    the timed work.
    """
    cpu_before = tree_cpu_seconds()
    start = time.perf_counter()
    result = fn()
    wall = time.perf_counter() - start
    cpu = max(0.0, tree_cpu_seconds() - cpu_before)
    return wall, cpu, result


def memory_over(fn, interval: float = 0.02):
    """Run fn() once with a memory sampler attached; return (avg_mb, max_mb).

    This run is for memory only -- its wall time is discarded -- so the sampler
    thread never overlaps a timed region.
    """
    samples: list[float] = []
    stop = False

    def sample_loop() -> None:
        while not stop:
            samples.append(tree_mem_mb())
            time.sleep(interval)

    thread = threading.Thread(target=sample_loop, daemon=True)
    thread.start()
    try:
        fn()
    finally:
        stop = True
        thread.join()
    samples.append(tree_mem_mb())
    return statistics.fmean(samples), max(samples)


def summarize(values: Sequence[float]) -> Summary:
    """Median, min, max and n for a list of numbers."""
    vs = sorted(values)
    return {"median": statistics.median(vs), "min": vs[0], "max": vs[-1],
            "n": len(vs), "runs": list(values)}


# --------------------------------------------------------------------------
# Result output
# --------------------------------------------------------------------------
def table(title: str, headers: Sequence[str], rows: Sequence[Sequence[object]],
          note: str = "") -> str:
    """Format a simple aligned table: first column left, the rest right."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def fmt(cells):
        parts = [str(c).ljust(widths[i]) if i == 0 else str(c).rjust(widths[i])
                 for i, c in enumerate(cells)]
        return "  ".join(parts).rstrip()

    lines = ["", title, "-" * 84]
    if note:
        lines.append(note)
    lines.append(fmt(headers))
    lines.extend(fmt(r) for r in rows)
    return "\n".join(lines)


def report(experiment: str, records: list[dict], title: str,
           headers: Sequence[str], rows: Sequence[Sequence[object]],
           note: str = "") -> None:
    """Write the result file when a path is given as argv[1]; otherwise print.

    Run standalone -> prints the table. Run under main.py (path given) ->
    writes {experiment, records, summary} to that path and prints nothing,
    so the tables only appear in results/summary.txt.
    """
    text = table(title, headers, rows, note)
    if len(sys.argv) > 1:
        with open(sys.argv[1], "w") as f:
            json.dump({"experiment": experiment, "records": records,
                       "summary": text}, f)
    else:
        print(text)
