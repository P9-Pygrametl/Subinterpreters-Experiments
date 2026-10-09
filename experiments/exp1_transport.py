"""Exp 1: one-way data transfer, transport x datatype x size.

The consumer is created before the sampler starts (fork-safe), and only the
transfer is timed. Raw runs are kept.
"""
import concurrent.interpreters as ci
import multiprocessing as mp
import os
import sys
from collections.abc import Callable
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics

TARGET_BYTES = 80_000_000
REPEATS = 7

COMBOS = [
    ("int", "small"), ("str", "small"), ("bytes", "small"),
    ("tuple", "medium"), ("list", "medium"), ("dict", "medium"),
    ("bytes", "large"),
]

SIZES = {"small": 8, "medium": 4096, "large": 1_000_000}

CTX = mp.get_context("fork")


def make_payload(kind: str, size: int) -> tuple[object, int]:
    """Return (object, approximate payload bytes) for the transport test."""
    if kind == "int":
        return 123456, size
    if kind == "str":
        return "x" * size, size
    if kind == "bytes":
        return b"x" * size, size
    if kind == "tuple":
        n = max(1, size // 8)
        return tuple(range(n)), n * 8
    if kind == "list":
        n = max(1, size // 8)
        return list(range(n)), n * 8
    if kind == "dict":
        n = max(1, size // 16)
        return {f"k{i}": i for i in range(n)}, n * 16
    raise ValueError(f"unknown payload kind: {kind}")


def _queue_consumer(queue: metrics.Queue, done: metrics.Queue, n: int) -> None:
    for _ in range(n):
        queue.get()
    done.put(1)


def _pipe_consumer(connection: Any, done: metrics.Queue, n: int) -> None:
    for _ in range(n):
        connection.recv()
    done.put(1)


def _interpreter_consumer(queue: metrics.Queue, done: metrics.Queue,
                          n: int) -> None:
    for _ in range(n):
        queue.get()
    done.put(1)


# --- per-transport setup/transfer/teardown ---------------------------------
def setup_interp_queue(payload: object, n: int) -> tuple[Callable[[], int], Callable[[], None]]:
    queue, done = ci.create_queue(), ci.create_queue()
    interpreter = ci.create()
    worker = interpreter.call_in_thread(_interpreter_consumer, queue, done, n)

    def transfer():
        for _ in range(n):
            queue.put(payload)
        done.get(timeout=300)
        return n

    def teardown():
        worker.join(300)
        interpreter.close()

    return transfer, teardown


def setup_mp_queue(payload: object, n: int) -> tuple[Callable[[], int], Callable[[], None]]:
    queue, done = CTX.Queue(), CTX.Queue()
    worker = CTX.Process(target=_queue_consumer, args=(queue, done, n))
    worker.start()

    def transfer():
        for _ in range(n):
            queue.put(payload)
        done.get(timeout=300)
        return n

    def teardown():
        worker.join(300)

    return transfer, teardown


def setup_mp_pipe(payload: object, n: int) -> tuple[Callable[[], int], Callable[[], None]]:
    sender, receiver = CTX.Pipe(duplex=True)
    done = CTX.Queue()
    worker = CTX.Process(target=_pipe_consumer, args=(receiver, done, n))
    worker.start()

    def transfer():
        for _ in range(n):
            sender.send(payload)
        done.get(timeout=300)
        return n

    def teardown():
        worker.join(300)

    return transfer, teardown


SETUPS = {"interp_queue": setup_interp_queue, "mp_queue": setup_mp_queue,
          "mp_pipe": setup_mp_pipe}


def measure_once(transport: str, kind: str, size: str) -> dict:
    payload, payload_bytes = make_payload(kind, SIZES[size])
    n = max(50, min(20000, TARGET_BYTES // max(1, payload_bytes)))

    # Memory: one separate, untimed pass.
    transfer, teardown = SETUPS[transport](payload, n)
    mem_avg, mem_max = metrics.memory_over(transfer)
    teardown()

    transfer_walls, cpu_times, correct = [], [], []
    for _ in range(REPEATS):
        # Create the worker before timing; nothing else runs during timed().
        transfer, teardown = SETUPS[transport](payload, n)
        wall, cpu, received = metrics.timed(transfer)
        teardown()
        transfer_walls.append(wall)
        cpu_times.append(cpu)
        correct.append(received == n)

    summary = metrics.summarize(transfer_walls)
    return {"transport": transport, "kind": kind, "size": size, "n": n,
            "payload_bytes": payload_bytes,
            "ok": all(correct), "transfer": summary,
            "cpu": metrics.summarize(cpu_times),
            "mem_avg": mem_avg, "mem_max": mem_max,
            "net_mb_per_s": payload_bytes * n / max(1e-9, summary["median"]) / 1e6,
            "net_msgs_per_s": n / max(1e-9, summary["median"])}


def main() -> None:
    records = []
    for transport in SETUPS:
        for kind, size in COMBOS:
            records.append(measure_once(transport, kind, size))

    rows = []
    for record in records:
        summary = record["transfer"]
        rows.append([record["transport"], record["kind"], record["size"],
                     record["n"], f"{summary['median']:.3f}s",
                     f"{record['net_mb_per_s']:.1f}",
                     f"{record['cpu']['median']:.2f}s",
                     f"{record['mem_avg']:.1f}MB",
                     f"{record['mem_max']:.1f}MB", record["ok"]])
    metrics.report(
        "exp1_transport", records,
        "Exp1 one-way transport",
        ["transport", "kind", "size", "n", "transfer", "net MB/s", "cpu",
         "mem_avg", "mem_max", "ok"],
        rows, note=f"target~{TARGET_BYTES/1e6:.0f}MB/run repeats={REPEATS}")


if __name__ == "__main__":
    main()
