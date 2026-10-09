# Subinterpreters vs Multiprocessing for pygrametl
## Motivation

Is there any scenario where subinterpreters beat multiprocessing for pygrametl?

## Method

### What we compare
Two mechanisms for CPU-level parallelism, run on identical workloads, without
any pygrametl code:

- **multiprocessing** — separate worker processes (tested with the `fork` start
  method, which is what pygrametl uses).
- **subinterpreters** — multiple interpreters in one process via
  `concurrent.interpreters`, `InterpreterPoolExecutor`, and `Interpreter.call`.

In both cases the workers are created **once and reused** across repetitions.
This separates the cost of starting workers from the cost of doing work, and it
avoids measuring worker startup as if it were part of the workload.

### Metrics

- **WALL** — `time.perf_counter()` around the work.
- **CPU** — CPU time of the whole process tree (parent + workers), via psutil
  `cpu_times()` summed over the tree. `time.process_time()` alone misses child
  processes.
- **MEM avg / max** — proportional set size (PSS) of the process tree, sampled in a
  separate, untimed pass so the sampler thread never competes with the timed work;
  PSS avoids double-counting pages shared between processes.
- **Correctness (`ok`)** — each experiment verifies its own result: messages
  received, expected round totals, computed values, or worker exit codes.

All four are recorded for the steady-state experiments (transport,
coordination, CPU scaling, shared buffer). The startup experiment has no
steady-state work, so it reports create/teardown time plus the memory added by
starting the workers.

### Protocol and controls

- Workers are reused across **rounds**; one round performs one full unit of work.
- **5–7 repetitions** per configuration, reported as **median with min..max**;
  the raw runs are stored in `results/results.json`. The startup experiment uses
  30 repetitions (its runs are cheap).
- **Startup** is measured only by `exp5_startup`, in isolation, so it is not
  folded into the other experiments' timings.
- The two mechanisms are **interleaved** within each worker count so that
  thermal/power drift is not attributed to a mechanism.
- Worker counts are **1, 2, 4, 8**.

### The experiments — what each tests, and why

1. **Transport** (`exp1_transport.py`) — how fast does data move from the
   producer to a worker, for different payload types and sizes?
   *Why:* pygrametl moves rows between steps, so transfer cost and which types
   each mechanism can send matter.
2. **Fan-out** (`exp2_fanout.py`) — one source hands many small tasks to W
   workers and collects the results.
   *Why:* this is the scatter-gather shape of spreading a transform across
   cores.
3. **Dict transfer sweep** (`exp3_dict_transport.py`) — throughput for a row
   dict as its number of columns grows, for each transport.
   *Why:* it finds the row size at which multiprocessing's queue or pipe overtake
   the interpreter queue -- i.e. when interpreters stop being the cheaper way to
   move pygrametl rows.
4. **Coordinator** (`exp4_coordinator.py`) — workers do a chunk of work, then ask
   a single owner for the next ID.
   *Why:* shared ID/key allocation is the coordination problem pygrametl
   actually has (`getsharedsequencefactory`), and subinterpreters have no shared
   `Value`/`Lock` — only queues — so it has to be compared directly.
5. **CPU scaling** (`exp5_cpu_memory.py`) — do CPU-bound transforms actually
   scale across cores and how do the two mechanisms compare?
   *Why:* this is the primary workload either mechanism exists for.
6. **Shared read-only buffer** (`exp6_shared_buffer.py`) — one large read-only
   buffer shared across workers, each reading a disjoint partition.
   *Why:* this is the scenario most favourable to subinterpreters (in-process
   buffer sharing), so it tests their best case against multiprocessing's
   `shared_memory` and against copying.
7. **Worker startup** (`exp7_startup.py`) — the cost of bringing workers up and
   tearing them down, with no work in between.
   *Why:* startup cost differs between the mechanisms and is paid for every
   short-lived pool, so it is measured in isolation rather than only as a
   byproduct of the other experiments.

## Setup and running

Requires Python 3.14+ (for `concurrent.interpreters`).

```bash
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/python main.py
```

This writes `results/summary.txt` (readable tables) and
`results/results.json` (all records, for charts).
