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


TASKS_PER_ROUND = 4000
WORK_PER_TASK = 2000
ROUNDS = 10

WORKER_COUNTS = [1, 2, 4, 8]

MP_CONTEXT = mp.get_context("fork")


# --- Worker (runs in a process or an interpreter) ----------------------

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


class FanoutBase:
    def round(self) -> int:
        for task in range(TASKS_PER_ROUND):
            self.tasks.put(task)

        for _ in range(self.worker_count):
            self.tasks.put("ROUND")

        received = finished = 0

        while received < TASKS_PER_ROUND or finished < self.worker_count:
            result = self.results.get(timeout=300)

            if result == "ROUND":
                finished += 1
            else:
                received += 1

        return received


# --- Multiprocessing ------------------------------------------------------

class FanoutProcesses(FanoutBase):
    def __init__(self, worker_count: int) -> None:
        self.worker_count = worker_count

        self.tasks, self.results = MP_CONTEXT.Queue(), MP_CONTEXT.Queue()

        self.processes = [
            MP_CONTEXT.Process(
                target=_fanout_worker,
                args=(self.tasks, self.results, WORK_PER_TASK),
            )
            for _ in range(worker_count)
        ]

        for process in self.processes:
            process.start()

    def stop(self) -> None:
        for _ in range(self.worker_count):
            self.tasks.put("STOP")

        for process in self.processes:
            process.join(timeout=300)

        # Terminate any workers that failed to stop.
        timed_out = False

        for process in self.processes:
            if process.is_alive():
                timed_out = True
                process.terminate()
                process.join(timeout=5)

                if process.is_alive():
                    process.kill()
                    process.join()

        for process in self.processes:
            process.close()

        self.tasks.close()
        self.results.close()

        if timed_out:
            raise RuntimeError("One or more worker processes failed to stop")


# --- Subinterpreters ------------------------------------------------------

class FanoutInterpreters(FanoutBase):
    def __init__(self, worker_count: int) -> None:
        self.worker_count = worker_count

        self.tasks, self.results = ci.create_queue(), ci.create_queue()
        self.interpreters, self.threads = [], []

        for _ in range(worker_count):
            interpreter = ci.create()
            thread = interpreter.call_in_thread(
                _fanout_worker,
                self.tasks,
                self.results,
                WORK_PER_TASK,
            )

            self.interpreters.append(interpreter)
            self.threads.append(thread)

    def stop(self) -> None:
        for _ in range(self.worker_count):
            self.tasks.put("STOP")

        for thread in self.threads:
            thread.join(timeout=300)

        # Do not close interpreters while their threads are running.
        if any(thread.is_alive() for thread in self.threads):
            raise RuntimeError("One or more interpreter threads failed to stop")

        for interpreter in self.interpreters:
            interpreter.close()


MECHANISMS = {
    "processes": FanoutProcesses,
    "interpreters": FanoutInterpreters,
}


def main() -> None:
    records = []
    randomizer = random.Random(7)

    for worker_count in WORKER_COUNTS:
        order = list(MECHANISMS)
        randomizer.shuffle(order)

        for name in order:
            session_class = MECHANISMS[name]
            session = session_class(worker_count)

            try:
                warm_result = session.round()
                ok = warm_result == TASKS_PER_ROUND

                mem_avg, mem_max = metrics.memory_over(session.round)

                round_walls = []
                cpu_total = 0.0

                for _ in range(ROUNDS):
                    wall, cpu, received = metrics.timed(session.round)

                    round_walls.append(wall)
                    cpu_total += cpu
                    ok = ok and received == TASKS_PER_ROUND
            finally:
                session.stop()

            round_summary = metrics.summarize(round_walls)

            records.append({
                "mechanism": name,
                "workers": worker_count,
                "steady": round_summary,
                "cpu": cpu_total,
                "mem_avg": mem_avg,
                "mem_max": mem_max,
                "ok": ok,
            })

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
        "exp2_fanout",
        records,
        "Exp2 fan-out (workers reused)",
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
            f"T={TASKS_PER_ROUND} "
            f"work/item={WORK_PER_TASK} "
            f"rounds={ROUNDS}"
        ),
    )


if __name__ == "__main__":
    main()
