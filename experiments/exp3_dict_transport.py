"""Exp 3: dict-size sweep for one-way transfer.

Each message is a row dict with a growing number of string-valued columns -- the
shape of a pygrametl row. This finds the size at which multiprocessing's queue
and pipe overtake the interpreter queue.
"""

import concurrent.interpreters as ci
import multiprocessing as mp
import os
import pickle
import random
import sys
from collections.abc import Callable
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics


MESSAGES_PER_RUN = 2000
REPEATS = 10

COLUMN_COUNTS = [1, 2, 5, 10, 20, 50]
VALUE = "x" * 20

MP_CONTEXT = mp.get_context("fork")


def make_row(column_count: int) -> tuple[dict, int]:
    row = {f"field{i}": VALUE for i in range(column_count)}
    return row, len(pickle.dumps(row))


# --- Consumers (one per transport), created before the sampler -------------

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


# --- Transport setup/transfer/teardown ------------------------------------

def setup_interp_queue(
    payload: object,
    message_count: int,
) -> tuple[Callable[[], int], Callable[[], None]]:
    queue, done = ci.create_queue(), ci.create_queue()
    interpreter = ci.create()

    worker = interpreter.call_in_thread(
        _queue_consumer, queue, done, message_count
    )

    def transfer():
        for _ in range(message_count):
            queue.put(payload)

        done.get(timeout=300)
        return message_count

    def teardown():
        worker.join(timeout=300)

        if worker.is_alive():
            raise RuntimeError("Subinterpreter worker failed to stop")

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

    def transfer():
        for _ in range(message_count):
            queue.put(payload)

        done.get(timeout=300)
        return message_count

    def teardown():
        worker.join(timeout=300)

        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=5)

            if worker.is_alive():
                worker.kill()
                worker.join()

            raise RuntimeError("Worker process failed to stop")

        worker.close()
        queue.close()
        done.close()

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

    def transfer():
        for _ in range(message_count):
            sender.send(payload)

        done.get(timeout=300)
        return message_count

    def teardown():
        worker.join(timeout=300)

        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=5)

            if worker.is_alive():
                worker.kill()
                worker.join()

            raise RuntimeError("Worker process failed to stop")

        worker.close()
        sender.close()
        receiver.close()
        done.close()

    return transfer, teardown


TRANSPORT_SETUPS = {
    "interp_queue": setup_interp_queue,
    "mp_queue": setup_mp_queue,
    "mp_pipe": setup_mp_pipe,
}


def measure_once(transport: str, column_count: int) -> dict:
    payload, payload_bytes = make_row(column_count)
    message_count = MESSAGES_PER_RUN

    # Memory: one separate, untimed pass.
    transfer, teardown = TRANSPORT_SETUPS[transport](payload, message_count)

    try:
        mem_avg, mem_max = metrics.memory_over(transfer)
    finally:
        teardown()

    transfer_walls = []
    cpu_times = []
    correct = []

    for _ in range(REPEATS):
        transfer, teardown = TRANSPORT_SETUPS[transport](
            payload, message_count
        )

        try:
            wall, cpu, received = metrics.timed(transfer)
        finally:
            teardown()

        transfer_walls.append(wall)
        cpu_times.append(cpu)
        correct.append(received == message_count)

    summary = metrics.summarize(transfer_walls)

    return {
        "transport": transport,
        "columns": column_count,
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
        "rows_per_s": message_count / max(1e-9, summary["median"]),
    }


def main() -> None:
    records = []
    randomizer = random.Random(31)

    for column_count in COLUMN_COUNTS:
        order = list(TRANSPORT_SETUPS)
        randomizer.shuffle(order)

        for transport in order:
            records.append(measure_once(transport, column_count))

    rows = []

    for record in records:
        summary = record["transfer"]

        rows.append([
            record["transport"],
            record["columns"],
            record["payload_bytes"],
            f"{summary['median']:.4f}s",
            f"{record['net_mb_per_s']:.1f}",
            f"{record['rows_per_s']:.0f}",
            f"{record['cpu']['median']:.2f}s",
            f"{record['mem_max']:.1f}MB",
            record["ok"],
        ])

    metrics.report(
        "exp3_dict_transport",
        records,
        "Exp3 dict transfer (columns swept)",
        [
            "transport",
            "columns",
            "bytes",
            "transfer",
            "MB/s",
            "rows/s",
            "cpu",
            "mem_max",
            "ok",
        ],
        rows,
        note=(
            f"messages={MESSAGES_PER_RUN} "
            f"repeats={REPEATS} "
            f"value={len(VALUE)} chars"
        ),
    )


if __name__ == "__main__":
    main()
