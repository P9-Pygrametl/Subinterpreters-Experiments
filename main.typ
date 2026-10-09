#import "@preview/lilaq:0.6.0" as lq

#let results = json("results/results.json")

#set text(size: 10pt)
#set page(paper: "a4", margin: 2cm)
#set par(justify: true)

// All records of one experiment.
#let recs(name) = results.at(name)

// Median of `field` for a group, sorted by worker count.
#let series(name, group, field: "steady") = {
  let rs = recs(name).filter(r => r.mechanism == group).sorted(key: r => r.workers)
  (rs.map(r => r.workers), rs.map(r => r.at(field).at("median")))
}


#align(center)[
  #text(size: 16pt, weight: "bold")[
    Subinterpreters vs multiprocessing for pygrametl parallelism
  ]
]

= Question

Is there any scenario where subinterpreters beat multiprocessing for
pygrametl? The experiments below compare the two mechanisms on the axes that
could differ: data transfer (including a row-size sweep), coordination (fan-out
and a shared ID owner), CPU scaling, sharing a large read-only buffer, and worker
startup. They are standalone (no pygrametl code), reuse their workers, and report
medians over
repeated runs.

= CPU-bound scaling

Workers are created once, then a fixed amount of pure calculation is split
across them. More workers should mean less wall time -- for both mechanisms.

#let seq = recs("exp5_cpu_memory").filter(r => r.mechanism == "sequential").first().steady.median
#let (cpu-xs-p, cpu-ys-p) = series("exp5_cpu_memory", "processes")
#let (cpu-xs-i, cpu-ys-i) = series("exp5_cpu_memory", "interpreters")

#lq.diagram(
  width: 11cm,
  height: 6.5cm,
  title: [CPU-bound scaling],
  xlabel: [workers],
  ylabel: [seconds per run],
  lq.plot((1, 8), (seq, seq), mark: none, label: [sequential]),
  lq.plot(cpu-xs-p, cpu-ys-p, mark: "s", label: [processes]),
  lq.plot(cpu-xs-i, cpu-ys-i, mark: "o", label: [interpreters]),
)

= Worker startup

Time to bring workers up and shut them down, doing no work. This is the clearest
difference: forking a process is much cheaper than creating an interpreter.

#let start = recs("exp7_startup")
#let start-fork = start.filter(r => r.mechanism == "fork").sorted(key: r => r.workers)
#let start-interp = start.filter(r => r.mechanism == "interpreters").sorted(key: r => r.workers)
#let start-ws = start-fork.map(r => r.workers)

#lq.diagram(
  width: 11cm,
  height: 6.5cm,
  title: [Worker startup cost],
  xlabel: [workers],
  ylabel: [ms to create all workers],
  lq.bar(start-ws, start-fork.map(r => r.create.median * 1000), offset: -0.2, width: 0.4, label: [fork]),
  lq.bar(start-ws, start-interp.map(r => r.create.median * 1000), offset: 0.2, width: 0.4, label: [interpreters]),
)

= Sharing a large read-only buffer

One 32 MB buffer is read by every worker, each taking a disjoint slice. The
question is how the two mechanisms compare when sharing it. (Copying the whole
buffer to every worker is also measured as a baseline; it is several times
slower, so it is left out of the chart.)

#let (buf-xs-i, buf-ys-i) = series("exp6_shared_buffer", "interpreters_shared")
#let (buf-xs-s, buf-ys-s) = series("exp6_shared_buffer", "processes_sharedmem")

#lq.diagram(
  width: 11cm,
  height: 6.5cm,
  title: [Shared read-only buffer],
  xlabel: [workers],
  ylabel: [seconds per round],
  lq.plot(buf-xs-i, buf-ys-i, mark: "o", label: [interpreters (shared)]),
  lq.plot(buf-xs-s, buf-ys-s, mark: "s", label: [processes (shared_memory)]),
)

= Coordination

Coordination in ETL takes a few shapes, so it is split into two experiments:
fan-out (many small tasks) and a coordinator (one owner handing out IDs).

== Fan-out: many small tasks

Handing many small tasks to workers scales well for both mechanisms.

#let (fan-xs-p, fan-ys-p) = series("exp2_fanout", "processes")
#let (fan-xs-i, fan-ys-i) = series("exp2_fanout", "interpreters")

#lq.diagram(
  width: 11cm,
  height: 6.5cm,
  title: [Fan-out: many small tasks],
  xlabel: [workers],
  ylabel: [seconds per round],
  lq.plot(fan-xs-p, fan-ys-p, mark: "s", label: [processes]),
  lq.plot(fan-xs-i, fan-ys-i, mark: "o", label: [interpreters]),
)

== Coordinator: one owner hands out IDs

Workers do a chunk of work, then ask a single owner for the next ID -- the shape
of pygrametl's shared-sequence ID generation. Every request and reply crosses a
queue, and by default an interpreter queue polls for an item only every 10 ms, so
request/reply coordination is very expensive for subinterpreters. Disabling that
poll (`interpreters_no_wait`) brings them level with processes -- confirming the
gap is the poll delay, not the coordination itself.

#let (cr-x-p, cr-y-p) = series("exp4_coordinator", "processes")
#let (cr-x-i, cr-y-i) = series("exp4_coordinator", "interpreters")
#let (cr-x-n, cr-y-n) = series("exp4_coordinator", "interpreters_no_wait")

#lq.diagram(
  width: 11cm,
  height: 6.5cm,
  title: [Coordinator: 1000 IDs per round],
  xlabel: [workers],
  ylabel: [seconds per round],
  lq.plot(cr-x-p, cr-y-p, mark: "s", label: [processes]),
  lq.plot(cr-x-i, cr-y-i, mark: "o", label: [interpreters (default)]),
  lq.plot(cr-x-n, cr-y-n, mark: "^", label: [interpreters (no wait)]),
)

= Data transfer

How fast a value moves from the producer to a worker. The magnitudes differ by
orders of magnitude, so the payloads are split across three charts (small
values, medium containers, one large payload) to keep the differences visible.

#let transport-throughput(transport, kind, size) = {
  recs("exp1_transport")
    .filter(r => r.transport == transport and r.kind == kind and r.size == size)
    .first()
    .net_mb_per_s
}

#let transport-diagram(title, payloads) = {
  let xs = range(payloads.len())
  let labels = payloads.map(p => p.at(0) + " " + p.at(1))
  lq.diagram(
    width: 11cm,
    height: 6.5cm,
    title: title,
    ylabel: [MB/s],
    xaxis: (ticks: xs.zip(labels)),
    lq.bar(
      xs,
      payloads.map(p => transport-throughput("interp_queue", p.at(0), p.at(1))),
      offset: -0.27,
      width: 0.25,
      label: [interpreters.Queue],
    ),
    lq.bar(
      xs,
      payloads.map(p => transport-throughput("mp_queue", p.at(0), p.at(1))),
      offset: 0.0,
      width: 0.25,
      label: [mp.Queue],
    ),
    lq.bar(
      xs,
      payloads.map(p => transport-throughput("mp_pipe", p.at(0), p.at(1))),
      offset: 0.27,
      width: 0.25,
      label: [mp.Pipe],
    ),
  )
}

#transport-diagram([Small values], (("int", "small"), ("str", "small"), ("bytes", "small")))
#transport-diagram([Medium containers], (("tuple", "medium"), ("list", "medium"), ("dict", "medium")))
#transport-diagram([Large payload], (("bytes", "large"),))

== Row dicts: processes win at every size

The same comparison for a row dict as its number of columns grows (values are
short strings). Unlike the tiny immutable values above, the process transports
move about twice as many rows per second as the interpreter queue at every size
tested, from one column to fifty -- the interpreter queue is not cheaper here.

#let dict-columns = (1, 2, 5, 10, 20, 50)

#let dict-rows-per-second(transport) = {
  let rs = recs("exp3_dict_transport").filter(r => r.transport == transport).sorted(key: r => r.columns)
  rs.map(r => r.rows_per_s)
}

#lq.diagram(
  width: 11cm,
  height: 6.5cm,
  title: [Dict transfer: rows/s vs columns],
  xlabel: [columns],
  ylabel: [rows/s],
  lq.plot(dict-columns, dict-rows-per-second("interp_queue"), mark: "o", label: [interpreters.Queue]),
  lq.plot(dict-columns, dict-rows-per-second("mp_queue"), mark: "s", label: [mp.Queue]),
  lq.plot(dict-columns, dict-rows-per-second("mp_pipe"), mark: "^", label: [mp.Pipe]),
)

= What the results say

- *CPU scaling:* both reach the same speedup; interpreters are marginally ahead,
  within run-to-run noise.
- *Startup:* multiprocessing wins clearly; forking a worker is far cheaper than
  creating an interpreter.
- *Shared read-only buffer:* a tie at two or more workers; both share it without
  the copying cost of the baseline.
- *Fan-out:* scales for both mechanisms; they are close.
- *Coordinator:* the default interpreter queue makes request/reply coordination
  ~10x more expensive than processes because it polls only every 10 ms; disabling
  that poll brings interpreters level with processes, so the penalty is the
  queue's poll delay, not coordination itself.
- *Data transfer:* the winner depends on the payload. Interpreters are faster for
  tiny immutable values (int/str/bytes), but for row dicts the process transports
  are about twice as fast at every size, and for large payloads clearly so.

Net: neither mechanism dominates. Processes win startup and moving row dicts;
interpreters are faster only for tiny immutable values; CPU-bound work, fan-out,
and sharing a large buffer are close to ties; and fine-grained coordination is
expensive for interpreters because of the interpreter queue's poll delay.



= Raw Results

#read("results/summary.txt")
