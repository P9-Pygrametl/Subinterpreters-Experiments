"""Exp 2: fan-out -- one source hands many small tasks to W workers and
collects the results.

Workers are reused across rounds.
"""
import concurrent.interpreters as ci
import multiprocessing as mp
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import metrics

T = 4000        # tasks per round
WORK = 2000     # tiny computation per task
ROUNDS = 5
WORKER_COUNTS = [1, 2, 4, 8]
CTX = mp.get_context("fork")


# --- the worker (runs in a process or an interpreter) ----------------------
def _fanout_worker(tasks, results, work):
    while True:
        task = tasks.get()
        if task == "STOP":
            break
        if task == "ROUND":
            results.put("ROUND")
            continue
        checksum = 0
        for i in range(work):
            checksum = (checksum * 31 + i) % 1000003
        results.put((task, checksum))


# --- the two mechanisms ----------------------------------------------------
class FanoutProcesses:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.tasks, self.results = CTX.Queue(), CTX.Queue()
        self.processes = [
            CTX.Process(target=_fanout_worker,
                        args=(self.tasks, self.results, WORK))
            for _ in range(workers)
        ]
        for process in self.processes:
            process.start()

    def round(self) -> int:
        for task in range(T):
            self.tasks.put(task)
        for _ in range(self.workers):
            self.tasks.put("ROUND")
        received = finished = 0
        while received < T or finished < self.workers:
            result = self.results.get()
            if result == "ROUND":
                finished += 1
            else:
                received += 1
        return received

    def stop(self) -> None:
        for _ in range(self.workers):
            self.tasks.put("STOP")
        for process in self.processes:
            process.join(300)


class FanoutInterpreters:
    def __init__(self, workers: int) -> None:
        self.workers = workers
        self.tasks, self.results = ci.create_queue(), ci.create_queue()
        self.interpreters, self.threads = [], []
        for _ in range(workers):
            interpreter = ci.create()
            thread = interpreter.call_in_thread(
                _fanout_worker, self.tasks, self.results, WORK)
            self.interpreters.append(interpreter)
            self.threads.append(thread)

    def round(self) -> int:
        for task in range(T):
            self.tasks.put(task)
        for _ in range(self.workers):
            self.tasks.put("ROUND")
        received = finished = 0
        while received < T or finished < self.workers:
            result = self.results.get(timeout=300)
            if result == "ROUND":
                finished += 1
            else:
                received += 1
        return received

    def stop(self) -> None:
        for _ in range(self.workers):
            self.tasks.put("STOP")
        for thread in self.threads:
            thread.join(300)
        for interpreter in self.interpreters:
            interpreter.close()


MECHANISMS = {"processes": FanoutProcesses, "interpreters": FanoutInterpreters}


def main() -> None:
    records = []
    randomizer = random.Random(7)
    for workers in WORKER_COUNTS:
        order = list(MECHANISMS)
        randomizer.shuffle(order)
        for name in order:
            session_class = MECHANISMS[name]

            session = session_class(workers)
            warm_result = session.round()  # warm + correctness check
            ok = warm_result == T
            mem_avg, mem_max = metrics.memory_over(session.round)  # untimed pass
            round_walls, cpu_total = [], 0.0
            for _ in range(ROUNDS):
                wall, cpu, _ = metrics.timed(session.round)
                round_walls.append(wall)
                cpu_total += cpu
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
        "exp2_fanout", records, "Exp2 fan-out (workers reused)",
        ["mechanism", "workers", "round", "min..max", "cpu", "mem_avg",
         "mem_max", "ok"],
        rows, note=f"T={T} work/item={WORK} rounds={ROUNDS}")


if __name__ == "__main__":
    main()
