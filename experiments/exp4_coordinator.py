"""Exp 4: coordinator -- workers do a chunk of work, then ask a single owner for
the next ID (the shape of pygrametl's shared-sequence ID generation).

Three mechanisms:
  processes            - processes, multiprocessing queues
  interpreters         - subinterpreters, interpreter queues (default 10 ms poll)
  interpreters_no_wait - subinterpreters, interpreter queues with _delay=0
"""
import concurrent.interpreters as ci
import multiprocessing as mp
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics

TOTAL_IDS = 1000     # IDs handed out per round (constant across worker counts)
WORK = 4000          # computation between ID requests
ROUNDS = 5
WORKER_COUNTS = [1, 2, 4, 8]
CTX = mp.get_context("fork")
EXPECTED_SUM = TOTAL_IDS * (TOTAL_IDS + 1) // 2
DEFAULT_DELAY = 0.01  # concurrent.interpreters.Queue's default poll delay


# --- process workers (no _delay concept) -----------------------------------
def _coordinator_worker(worker_id, control, requests, reply, done, work):
    while True:
        count = control.get()
        if count is None:
            break
        id_sum = 0
        for _ in range(count):
            checksum = 0
            for i in range(work):
                checksum = (checksum * 31 + i) % 1000003
            requests.put(worker_id)
            id_sum += reply.get()
        done.put(id_sum)


def _coordinator_owner(requests, replies):
    counter = 0
    while True:
        worker_id = requests.get()
        if worker_id == "STOP":
            break
        counter += 1
        replies[worker_id].put(counter)


# --- interpreter workers (delay configurable) ------------------------------
def _coordinator_worker_interp(worker_id, control, requests, reply, done, work,
                               delay):
    while True:
        count = control.get(_delay=delay)
        if count is None:
            break
        id_sum = 0
        for _ in range(count):
            checksum = 0
            for i in range(work):
                checksum = (checksum * 31 + i) % 1000003
            requests.put(worker_id, _delay=delay)
            id_sum += reply.get(_delay=delay)
        done.put(id_sum, _delay=delay)


def _coordinator_owner_interp(requests, replies, delay):
    counter = 0
    while True:
        worker_id = requests.get(_delay=delay)
        if worker_id == "STOP":
            break
        counter += 1
        replies[worker_id].put(counter, _delay=delay)


class CoordinatorProcesses:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.per_worker = TOTAL_IDS // workers
        self.requests = CTX.Queue()
        self.replies = [CTX.Queue() for _ in range(workers)]
        self.controls = [CTX.Queue() for _ in range(workers)]
        self.dones = [CTX.Queue() for _ in range(workers)]
        self.owner = CTX.Process(target=_coordinator_owner,
                                 args=(self.requests, self.replies))
        self.owner.start()
        self.processes = [
            CTX.Process(target=_coordinator_worker,
                        args=(i, self.controls[i], self.requests,
                              self.replies[i], self.dones[i], WORK))
            for i in range(workers)
        ]
        for process in self.processes:
            process.start()

    def round(self) -> int:
        for i in range(self.workers):
            self.controls[i].put(self.per_worker)
        return sum(self.dones[i].get(timeout=600) for i in range(self.workers))

    def stop(self) -> None:
        for i in range(self.workers):
            self.controls[i].put(None)

        for process in self.processes:
            process.join(timeout=600)

        self.requests.put("STOP")
        self.owner.join(timeout=600)

        timed_out = False

        for process in [*self.processes, self.owner]:
            if process.is_alive():
                timed_out = True
                process.terminate()
                process.join(timeout=5)

                if process.is_alive():
                    process.kill()
                    process.join()

            process.close()

        for queue in [self.requests, *self.replies, *self.controls, *self.dones]:
            queue.close()

        if timed_out:
            raise RuntimeError("One or more coordinator processes failed to stop")


class CoordinatorInterpreters:
    def __init__(self, workers: int, delay: float = DEFAULT_DELAY) -> None:
        self.workers = workers
        self.delay = delay
        self.per_worker = TOTAL_IDS // workers
        self.requests = ci.create_queue()
        self.replies = [ci.create_queue() for _ in range(workers)]
        self.controls = [ci.create_queue() for _ in range(workers)]
        self.dones = [ci.create_queue() for _ in range(workers)]
        self.owner = ci.create()
        self.owner_thread = self.owner.call_in_thread(
            _coordinator_owner_interp, self.requests, self.replies, delay)
        self.interpreters, self.threads = [], []
        for i in range(workers):
            interpreter = ci.create()
            thread = interpreter.call_in_thread(
                _coordinator_worker_interp, i, self.controls[i], self.requests,
                self.replies[i], self.dones[i], WORK, delay)
            self.interpreters.append(interpreter)
            self.threads.append(thread)

    def round(self) -> int:
        for i in range(self.workers):
            self.controls[i].put(self.per_worker, _delay=self.delay)
        return sum(self.dones[i].get(_delay=self.delay)
                   for i in range(self.workers))

    def stop(self) -> None:
        for i in range(self.workers):
            self.controls[i].put(None, _delay=self.delay)

        for thread in self.threads:
            thread.join(timeout=600)

        if any(thread.is_alive() for thread in self.threads):
            raise RuntimeError("One or more interpreter workers failed to stop")

        for interpreter in self.interpreters:
            interpreter.close()

        self.requests.put("STOP", _delay=self.delay)
        self.owner_thread.join(timeout=600)

        if self.owner_thread.is_alive():
            raise RuntimeError("Coordinator interpreter failed to stop")

        self.owner.close()


MECHANISMS = {
    "processes": CoordinatorProcesses,
    "interpreters": lambda workers: CoordinatorInterpreters(workers, DEFAULT_DELAY),
    "interpreters_no_wait": lambda workers: CoordinatorInterpreters(workers, 0.0),
}


def main() -> None:
    records = []
    randomizer = random.Random(23)
    for workers in WORKER_COUNTS:
        order = list(MECHANISMS)
        randomizer.shuffle(order)
        for name in order:
            session = MECHANISMS[name](workers)

            try:
                warm_sum = session.round()
                ok = warm_sum == EXPECTED_SUM
                mem_avg, mem_max = metrics.memory_over(session.round)
                round_walls, cpu_total = [], 0.0
                for _ in range(ROUNDS):
                    wall, cpu, _ = metrics.timed(session.round)
                    round_walls.append(wall)
                    cpu_total += cpu
            finally:
                session.stop()

            round_summary = metrics.summarize(round_walls)
            records.append({"mechanism": name, "workers": workers,
                            "steady": round_summary,
                            "cpu": cpu_total,
                            "mem_avg": mem_avg,
                            "mem_max": mem_max, "ok": ok})

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
        "exp4_coordinator", records, "Exp4 coordinator (one owner hands out IDs)",
        ["mechanism", "workers", "round", "min..max", "cpu", "mem_avg",
         "mem_max", "ok"],
        rows, note=f"ids/round={TOTAL_IDS} work/request={WORK} rounds={ROUNDS}")


if __name__ == "__main__":
    main()
