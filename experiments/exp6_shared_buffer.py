"""Exp 6: share one large read-only buffer across N workers, each reading a
disjoint partition. The scenario most favourable to subinterpreters (zero-copy
sharing).

Workers are reused across rounds; raw rounds are kept.

Mechanisms:
  interpreters_shared  - interpreters; buffer passed by memoryview  [zero-copy]
  processes_sharedmem  - multiprocessing.shared_memory (attached once) [zero-copy]
"""

import array
import concurrent.interpreters as ci
import multiprocessing as mp
import multiprocessing.shared_memory as shared_memory_module
import os
import random
import sys
import threading
from concurrent.futures import ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics


ELEMENT_COUNT = 4_000_000
CHUNK_SIZE = 25_000
CHUNK_COUNT = (ELEMENT_COUNT + CHUNK_SIZE - 1) // CHUNK_SIZE

STEADY_REPS = 10
WORKER_COUNTS = [1, 2, 4, 8]

MP_CONTEXT = mp.get_context("fork")
EXPECTED_SUM = ELEMENT_COUNT * (ELEMENT_COUNT - 1) // 2


# --- workers (module-level, so they can run in a process or an interpreter) --
_SHARED_VIEW = None
_SHARED_MEMORY = None


def _init_shared_memory(name: str) -> None:
    # Keep the SharedMemory object in a global; otherwise it can be
    # garbage-collected while _SHARED_VIEW still exports from it (BufferError).
    global _SHARED_VIEW, _SHARED_MEMORY

    _SHARED_MEMORY = shared_memory_module.SharedMemory(name=name)
    _SHARED_VIEW = memoryview(_SHARED_MEMORY.buf).cast("q")

    def _release():
        try:
            _SHARED_VIEW.release()
        except Exception:  # noqa: BLE001, S110
            pass

    import atexit

    atexit.register(_release)


def _shared_memory_round(item: tuple[int, int]) -> int:
    index, worker_count = item
    data = _SHARED_VIEW
    total = 0

    for chunk in range(index, CHUNK_COUNT, worker_count):
        start = chunk * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, ELEMENT_COUNT)
        subtotal = 0

        for k in range(start, end):
            subtotal += data[k]

        total += subtotal

    return total


def _interpreter_chunk(
    args: tuple[memoryview, int, int, int, int, int],
) -> int:
    data, index, worker_count, chunk_count, chunk_size, element_count = args
    total = 0

    for chunk in range(index, chunk_count, worker_count):
        start = chunk * chunk_size
        end = min(start + chunk_size, element_count)
        subtotal = 0

        for k in range(start, end):
            subtotal += data[k]

        total += subtotal

    return total


class InterpreterShareSession:
    def __init__(self, buffer: memoryview, worker_count: int) -> None:
        self.buffer = buffer
        self.worker_count = worker_count
        self.interpreters = [ci.create() for _ in range(worker_count)]

    def round(self) -> int:
        results = [0] * self.worker_count
        errors = [None] * self.worker_count

        def work(index):
            try:
                results[index] = self.interpreters[index].call(
                    _interpreter_chunk,
                    (
                        self.buffer,
                        index,
                        self.worker_count,
                        CHUNK_COUNT,
                        CHUNK_SIZE,
                        ELEMENT_COUNT,
                    ),
                )
            except Exception as exc:
                errors[index] = exc

        threads = [
            threading.Thread(target=work, args=(index,))
            for index in range(self.worker_count)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        for error in errors:
            if error is not None:
                raise error

        return sum(results)

    def stop(self) -> None:
        for interpreter in self.interpreters:
            interpreter.close()


class SharedMemorySession:
    def __init__(self, name: str, worker_count: int) -> None:
        self.worker_count = worker_count
        self.executor = ProcessPoolExecutor(
            max_workers=worker_count,
            mp_context=MP_CONTEXT,
            initializer=_init_shared_memory,
            initargs=(name,),
        )

    def round(self) -> int:
        tasks = [
            (index, self.worker_count)
            for index in range(self.worker_count)
        ]

        return sum(
            self.executor.map(_shared_memory_round, tasks, chunksize=1)
        )

    def stop(self) -> None:
        self.executor.shutdown()


SESSIONS = {
    "interpreters_shared": lambda buffer, worker_count: InterpreterShareSession(
        buffer, worker_count
    ),
    "processes_sharedmem": lambda buffer, worker_count: SharedMemorySession(
        SHARED_MEMORY.name, worker_count
    ),
}


def main() -> None:
    global SHARED_MEMORY

    data = array.array("q", range(ELEMENT_COUNT))
    buffer = memoryview(data)
    raw_bytes = data.tobytes()

    SHARED_MEMORY = shared_memory_module.SharedMemory(
        create=True,
        size=len(raw_bytes),
    )
    SHARED_MEMORY.buf[:len(raw_bytes)] = raw_bytes

    records = []

    try:
        randomizer = random.Random(99)

        for worker_count in WORKER_COUNTS:
            order = list(SESSIONS)
            randomizer.shuffle(order)

            for name in order:
                session = SESSIONS[name](buffer, worker_count)

                try:
                    warm_result = session.round()  # warm + correctness check
                    ok = warm_result == EXPECTED_SUM

                    mem_avg, mem_max = metrics.memory_over(session.round)  # untimed

                    round_walls = []
                    cpu_total = 0.0

                    for _ in range(STEADY_REPS):
                        wall, cpu, _ = metrics.timed(session.round)

                        round_walls.append(wall)
                        cpu_total += cpu
                finally:
                    session.stop()

                records.append({
                    "mechanism": name,
                    "workers": worker_count,
                    "steady": metrics.summarize(round_walls),
                    "cpu": cpu_total,
                    "mem_avg": mem_avg,
                    "mem_max": mem_max,
                    "ok": ok,
                })
    finally:
        SHARED_MEMORY.close()
        SHARED_MEMORY.unlink()

    rows = []

    for record in records:
        summary = record["steady"]

        rows.append([
            record["mechanism"],
            record["workers"],
            f"{summary['median']:.3f}s",
            f"{summary['min']:.3f}..{summary['max']:.3f}",
            f"{record['cpu']:.2f}s",
            f"{record['mem_avg']:.1f}MB",
            f"{record['mem_max']:.1f}MB",
            record["ok"],
        ])

    metrics.report(
        "exp6_shared_buffer",
        records,
        "Exp6 shared read-only buffer (partitioned access, workers reused)",
        [
            "mechanism",
            "workers",
            "round",
            "min..max",
            "cpu",
            "mem_avg",
            "mem_max",
            "ok",
        ],
        rows,
        note=(
            f"N={ELEMENT_COUNT} int64 ({ELEMENT_COUNT * 8 / 1e6:.0f}MB) "
            f"chunks={CHUNK_COUNT} "
            f"steady_reps={STEADY_REPS}"
        ),
    )


if __name__ == "__main__":
    main()
