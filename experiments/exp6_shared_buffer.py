"""Exp 6: share one large read-only buffer across N workers, each reading a
disjoint partition. The scenario most favourable to subinterpreters (zero-copy
sharing).

Workers are reused across rounds; raw rounds are kept.

Mechanisms:
  interpreters_shared  - interpreters; buffer passed by memoryview  [zero-copy]
  processes_sharedmem  - multiprocessing.shared_memory (attached once) [zero-copy]
  processes_copy       - buffer pickled to each worker every round  [copies]
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

N = 4_000_000
CHUNK = 25_000
CHUNKS = (N + CHUNK - 1) // CHUNK
STEADY_REPS = 5
WORKER_COUNTS = [1, 2, 4, 8]
CTX = mp.get_context("fork")
EXPECTED = N * (N - 1) // 2


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
    index, workers = item
    data = _SHARED_VIEW
    total = 0
    for chunk in range(index, CHUNKS, workers):
        start = chunk * CHUNK
        end = min(start + CHUNK, N)
        subtotal = 0
        for k in range(start, end):
            subtotal += data[k]
        total += subtotal
    return total


def _copy_round(item: tuple[bytes, int, int]) -> int:
    raw, index, workers = item
    values = array.array("q")
    values.frombytes(raw)
    total = 0
    for chunk in range(index, CHUNKS, workers):
        start = chunk * CHUNK
        end = min(start + CHUNK, N)
        subtotal = 0
        for k in range(start, end):
            subtotal += values[k]
        total += subtotal
    return total


def _interpreter_chunk(args: tuple[memoryview, int, int, int, int, int]) -> int:
    data, index, workers, chunks, chunk_size, nelem = args
    total = 0
    for chunk in range(index, chunks, workers):
        start = chunk * chunk_size
        end = min(start + chunk_size, nelem)
        subtotal = 0
        for k in range(start, end):
            subtotal += data[k]
        total += subtotal
    return total


class InterpreterShareSession:
    def __init__(self, buffer: memoryview, workers: int) -> None:
        self.buffer = buffer
        self.workers = workers
        self.interpreters = [ci.create() for _ in range(workers)]

    def round(self) -> int:
        results = [0] * self.workers
        errors = [None] * self.workers

        def work(index):
            try:
                results[index] = self.interpreters[index].call(
                    _interpreter_chunk,
                    (self.buffer, index, self.workers, CHUNKS, CHUNK, N))
            except Exception as exc:
                errors[index] = exc

        threads = [threading.Thread(target=work, args=(index,))
                for index in range(self.workers)]
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
    def __init__(self, name: str, workers: int) -> None:
        self.workers = workers
        self.executor = ProcessPoolExecutor(
            max_workers=workers, mp_context=CTX,
            initializer=_init_shared_memory, initargs=(name,))

    def round(self) -> int:
        tasks = [(index, self.workers) for index in range(self.workers)]
        return sum(self.executor.map(_shared_memory_round, tasks, chunksize=1))

    def stop(self) -> None:
        self.executor.shutdown()


class CopySession:
    def __init__(self, raw: bytes, workers: int) -> None:
        self.raw = raw
        self.workers = workers
        self.executor = ProcessPoolExecutor(max_workers=workers, mp_context=CTX)

    def round(self) -> int:
        tasks = [(self.raw, index, self.workers)
                 for index in range(self.workers)]
        return sum(self.executor.map(_copy_round, tasks, chunksize=1))

    def stop(self) -> None:
        self.executor.shutdown()


SESSIONS = {
    "interpreters_shared": lambda buffer, workers: InterpreterShareSession(
        buffer, workers),
    "processes_sharedmem": lambda buffer, workers: SharedMemorySession(
        SHARED_MEMORY.name, workers),
    "processes_copy": lambda buffer, workers: CopySession(RAW_BYTES, workers),
}


def main() -> None:
    global SHARED_MEMORY, RAW_BYTES
    data = array.array("q", range(N))
    buffer = memoryview(data)
    RAW_BYTES = data.tobytes()
    SHARED_MEMORY = shared_memory_module.SharedMemory(create=True,
                                                      size=len(RAW_BYTES))
    SHARED_MEMORY.buf[:len(RAW_BYTES)] = RAW_BYTES

    records = []
    try:
        randomizer = random.Random(99)
        for workers in WORKER_COUNTS:
            order = list(SESSIONS)
            randomizer.shuffle(order)
            for name in order:
                session = SESSIONS[name](buffer, workers)
                try:
                    warm_result = session.round()  # warm + correctness check
                    ok = warm_result == EXPECTED
                    mem_avg, mem_max = metrics.memory_over(session.round)  # untimed
                    round_walls, cpu_total = [], 0.0
                    for _ in range(STEADY_REPS):
                        wall, cpu, _ = metrics.timed(session.round)
                        round_walls.append(wall)
                        cpu_total += cpu
                finally:
                    session.stop()

                records.append({"mechanism": name, "workers": workers,
                                "steady": metrics.summarize(round_walls),
                                "cpu": cpu_total,
                                "mem_avg": mem_avg,
                                "mem_max": mem_max, "ok": ok})
    finally:
        SHARED_MEMORY.close()
        SHARED_MEMORY.unlink()

    rows = []
    for record in records:
        summary = record["steady"]
        rows.append([record["mechanism"], record["workers"],
                     f"{summary['median']:.3f}s",
                     f"{summary['min']:.3f}..{summary['max']:.3f}",
                     f"{record['cpu']:.2f}s",
                     f"{record['mem_avg']:.1f}MB",
                     f"{record['mem_max']:.1f}MB", record["ok"]])
    metrics.report(
        "exp6_shared_buffer", records,
        "Exp6 shared read-only buffer (partitioned access, workers reused)",
        ["mechanism", "workers", "round", "min..max", "cpu", "mem_avg",
         "mem_max", "ok"],
        rows,
        note=f"N={N} int64 ({N * 8 / 1e6:.0f}MB) chunks={CHUNKS} "
             f"steady_reps={STEADY_REPS}")


if __name__ == "__main__":
    main()
