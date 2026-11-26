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
REPEATS = 10

COMBOS = [
    ("int", "small"),
    ("str", "small"),
    ("bytes", "small"),
    ("tuple", "medium"),
    ("list", "medium"),
    ("dict", "medium"),
    ("bytes", "large"),
]

SIZES = {
    "small": 8,
    "medium": 4096,
    "large": 1_000_000,
}

MP_CONTEXT = mp.get_context("fork")


def make_payload(kind: str, size: int) -> tuple[object, int]:
    """Return (object, approximate payload bytes) for the transport test."""
    if kind == "int":
        return 123456, size

    if kind == "str":
        return "x" * size, size

    if kind == "bytes":
        return b"x" * size, size

    if kind in ("tuple", "list"):
        count = max(1, size // 8)
        payload = tuple(range(count)) if kind == "tuple" else list(range(count))
        return payload, count * 8

    if kind == "dict":
        count = max(1, size // 16)
        return {f"k{i}": i for i in range(count)}, count * 16

    raise ValueError(f"unknown payload kind: {kind}")


def _queue_consumer(
    queue: metrics.Queue,
    done: metrics.Queue,
    message_count: int,
) -> None:
    for _ in range(message_count):
        queue.get()

    done.put(1)


def _pipe_consumer(
    connection: Any,
    done: metrics.Queue,
    message_count: int,
) -> None:
    for _ in range(message_count):
        connection.recv()

    done.put(1)


# --- Per-transport setup/transfer/teardown ----------------------------------

def setup_interp_queue(
    payload: object,
    message_count: int,
) -> tuple[Callable[[], int], Callable[[], None]]:
    queue, done = ci.create_queue(), ci.create_queue()
    interpreter = ci.create()

    worker = interpreter.call_in_thread(
        _queue_consumer, queue, done, message_count
    )

    def transfer() -> int:
        for _ in range(message_count):
            queue.put(payload)

        done.get(timeout=300)
        return message_count

    def teardown() -> None:
        worker.join(300)
        interpreter.close()

    return transfer, teardown


def setup_mp_queue(
    payload: object,
    message_count: int,
) -> tuple[Callable[[], int], Callable[[], None]]:
    queue, done = MP_CONTEXT.Queue(), MP_CONTEXT.Queue()

    worker = MP_CONTEXT.Process(
        target=_queue_consumer,
        args=(queue, done, message_count),
    )
    worker.start()

    def transfer() -> int:
        for _ in range(message_count):
            queue.put(payload)

        done.get(timeout=300)
        return message_count

    def teardown() -> None:
        worker.join(300)

    return transfer, teardown


def setup_mp_pipe(
    payload: object,
    message_count: int,
) -> tuple[Callable[[], int], Callable[[], None]]:
    sender, receiver = MP_CONTEXT.Pipe(duplex=True)
    done = MP_CONTEXT.Queue()

    worker = MP_CONTEXT.Process(
        target=_pipe_consumer,
        args=(receiver, done, message_count),
    )
    worker.start()

    def transfer() -> int:
        for _ in range(message_count):
            sender.send(payload)

        done.get(timeout=300)
        return message_count

    def teardown() -> None:
        worker.join(300)

    return transfer, teardown


TRANSPORT_SETUPS = {
    "interp_queue": setup_interp_queue,
    "mp_queue": setup_mp_queue,
    "mp_pipe": setup_mp_pipe,
}


def measure_once(transport: str, kind: str, size: str) -> dict:
    payload, payload_bytes = make_payload(kind, SIZES[size])
    message_count = max(50, min(20_000, TARGET_BYTES // payload_bytes))

    # Memory: one separate, untimed pass.
    transfer, teardown = TRANSPORT_SETUPS[transport](payload, message_count)
    mem_avg, mem_max = metrics.memory_over(transfer)
    teardown()

    transfer_walls = []
    cpu_times = []
    correct = []

    for _ in range(REPEATS):
        # Create the worker before timing; nothing else runs during timed().
        transfer, teardown = TRANSPORT_SETUPS[transport](payload, message_count)

        wall, cpu, received = metrics.timed(transfer)
        teardown()

        transfer_walls.append(wall)
        cpu_times.append(cpu)
        correct.append(received == message_count)

    summary = metrics.summarize(transfer_walls)

    return {
        "transport": transport,
        "kind": kind,
        "size": size,
        "n": message_count,
        "payload_bytes": payload_bytes,
        "ok": all(correct),
        "transfer": summary,
        "cpu": metrics.summarize(cpu_times),
        "mem_avg": mem_avg,
        "mem_max": mem_max,
        "net_mb_per_s": (
            payload_bytes * message_count
            / max(1e-9, summary["median"])
            / 1e6
        ),
        "net_msgs_per_s": message_count / max(1e-9, summary["median"]),
    }


def main() -> None:
    records = []

    for transport in TRANSPORT_SETUPS:
        for kind, size in COMBOS:
            records.append(measure_once(transport, kind, size))

    rows = []

    for record in records:
        summary = record["transfer"]

        rows.append([
            record["transport"],
            record["kind"],
            record["size"],
            record["n"],
            f"{summary['median']:.3f}s",
            f"{record['net_mb_per_s']:.1f}",
            f"{record['cpu']['median']:.2f}s",
            f"{record['mem_avg']:.1f}MB",
            f"{record['mem_max']:.1f}MB",
            record["ok"],
        ])

    metrics.report(
        "exp1_transport",
        records,
        "Exp1 one-way transport",
        [
            "transport",
            "kind",
            "size",
            "n",
            "transfer",
            "net MB/s",
            "cpu",
            "mem_avg",
            "mem_max",
            "ok",
        ],
        rows,
        note=f"target~{TARGET_BYTES / 1e6:.0f}MB/run repeats={REPEATS}",
    )


if __name__ == "__main__":
    main()
