#import "@preview/lilaq:0.6.0" as lq

#let results = json("results/results.json")

#set text(size: 10pt)
#set page(paper: "a4", margin: 1.8cm)
#set par(justify: true)

// All records of one experiment.
#let recs(name) = results.at(name)

// The experiments only test these worker counts. Plot at positions 0..3 and
// label with the real counts so the tested values are equally spaced.
#let worker-counts = (1, 2, 4, 8)
#let worker-ticks = worker-counts.enumerate().map(((i, w)) => (i, str(w)))
#let workers-to-pos(ws) = ws.map(w => worker-counts.position(x => x == w))

// Median of `field` for a group, sorted by worker count. The x values are
// mapped to evenly spaced positions (0..3).
#let series(name, group, field: "steady") = {
  let rs = recs(name).filter(r => r.mechanism == group).sorted(key: r => r.workers)
  (workers-to-pos(rs.map(r => r.workers)), rs.map(r => r.at(field).at("median")))
}

// Memory series (MB) for a group, sorted by worker count. `field` is mem_avg / mem_max.
#let mem-series(name, group, field) = {
  let rs = recs(name).filter(r => r.mechanism == group).sorted(key: r => r.workers)
  (workers-to-pos(rs.map(r => r.workers)), rs.map(r => r.at(field)))
}

// Figure with a caption. `body` is a full-width or half-width diagram.
#let fig(title, body, caption) = figure(
  caption: caption,
  kind: figure,
  supplement: [Figure],
  {
    align(center, body)
    v(0.35em)
  },
)

// Solo chart: full text width.
#let solo(title, caption, ..args) = fig(
  title,
  lq.diagram(
    width: 15cm,
    height: 6.6cm,
    title: title,
    ..args.named(),
    ..args.pos(),
  ),
  caption,
)

// Paired chart: half text width, for two side-by-side.
#let half(title, caption, ..args) = fig(
  title,
  lq.diagram(
    width: 7.2cm,
    height: 5.4cm,
    title: title,
    ..args.named(),
    ..args.pos(),
  ),
  caption,
)


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
medians over repeated runs. Each figure carries its own caption; the takeaway is
stated beneath it.

#let seq = recs("exp5_cpu_memory").filter(r => r.mechanism == "sequential").first().steady.median

= CPU-bound scaling

Workers are created once, then a fixed amount of pure calculation is split
across them. More workers should mean less wall time -- for both mechanisms.
The second chart shows what that speed costs in memory.

#let (cpu-xs-p, cpu-ys-p) = series("exp5_cpu_memory", "processes")
#let (cpu-xs-i, cpu-ys-i) = series("exp5_cpu_memory", "interpreters")

#grid(
  columns: (1fr, 1fr),
  gutter: 0.4cm,
  half(
    [CPU-bound scaling],
    [
      Wall time per run vs worker count. Ideal speedup is the `sequential`
      baseline; both mechanisms track it closely up to 8 workers.
    ],
    xlabel: [workers],
    ylabel: [seconds per run],
    xaxis: (ticks: worker-ticks, subticks: none),
    lq.plot((0, 3), (seq, seq), mark: none, label: [sequential]),
    lq.plot(cpu-xs-p, cpu-ys-p, mark: "s", label: [processes]),
    lq.plot(cpu-xs-i, cpu-ys-i, mark: "o", label: [interpreters]),
  ),
  half(
    [Memory during CPU scaling],
    [
      Resident memory vs worker count. Processes stay well below interpreters and
      grow slower, so the CPU tie is not memory-neutral.
    ],
    xlabel: [workers],
    ylabel: [MB (avg / peak)],
    xaxis: (ticks: worker-ticks, subticks: none),
    lq.plot(cpu-xs-p, mem-series("exp5_cpu_memory", "processes", "mem_avg").at(1), mark: "s", label: [processes (avg)]),
    lq.plot(
      cpu-xs-i,
      mem-series("exp5_cpu_memory", "interpreters", "mem_avg").at(1),
      mark: "o",
      label: [interpreters (avg)],
    ),
    lq.plot(
      cpu-xs-p,
      mem-series("exp5_cpu_memory", "processes", "mem_max").at(1),
      mark: none,
      stroke: (dash: "dashed"),
      label: [processes (peak)],
    ),
    lq.plot(
      cpu-xs-i,
      mem-series("exp5_cpu_memory", "interpreters", "mem_max").at(1),
      mark: none,
      stroke: (dash: "dashed"),
      label: [interpreters (peak)],
    ),
  ),
)

both reach the same speedup, within run-to-run noise at every worker count. But
interpreters hold roughly twice the memory at every worker count.

= Worker startup

Time to bring workers up and shut them down, doing no work. This is the clearest
difference: forking a process is much cheaper than creating an interpreter.

#let start = recs("exp7_startup")
#let start-fork = start.filter(r => r.mechanism == "fork").sorted(key: r => r.workers)
#let start-interp = start.filter(r => r.mechanism == "interpreters").sorted(key: r => r.workers)
#let start-ws = workers-to-pos(start-fork.map(r => r.workers))

#solo(
  [Worker startup cost],
  [
    Time to create all workers (no work done). Forking is flat and cheap;
    interpreter creation grows roughly linearly with worker count.
  ],
  xlabel: [workers],
  ylabel: [ms to create all workers],
  xaxis: (ticks: worker-ticks, subticks: none),
  lq.bar(start-ws, start-fork.map(r => r.create.median * 1000), offset: -0.2, width: 0.4, label: [fork]),
  lq.bar(start-ws, start-interp.map(r => r.create.median * 1000), offset: 0.2, width: 0.4, label: [interpreters]),
)

multiprocessing wins clearly. At 8 workers forking costs ~6 ms
versus ~101 ms for interpreters -- roughly 16x.

= Sharing a large read-only buffer

One 32 MB buffer is read by every worker, each taking a disjoint slice. The
question is how the two mechanisms compare when sharing it.

#let (buf-xs-i, buf-ys-i) = series("exp6_shared_buffer", "interpreters_shared")
#let (buf-xs-s, buf-ys-s) = series("exp6_shared_buffer", "processes_sharedmem")

#solo(
  [Shared read-only buffer],
  [
    Wall time per round. Sharing is a tie at two or more workers: the two
    mechanisms track each other closely once workers are actually parallel.
  ],
  xlabel: [workers],
  ylabel: [seconds per round],
  xaxis: (ticks: worker-ticks, subticks: none),
  lq.plot(buf-xs-i, buf-ys-i, mark: "o", label: [interpreters (shared)]),
  lq.plot(buf-xs-s, buf-ys-s, mark: "s", label: [processes (shared_memory)]),
)

sharing the buffer is a tie: subinterpreters and `shared_memory` land within a
few percent of each other on both time and memory (about 140 MB at 8 workers),
so neither is favoured by large read-only sharing.

= Coordination

Coordination in ETL takes a few shapes, so it is split into two experiments:
fan-out (many small tasks) and a coordinator (one owner handing out IDs).

== Fan-out: many small tasks

Handing many small tasks to workers scales well for both mechanisms;
interpreters are a little ahead throughout, but the margin is small.

#let (fan-xs-p, fan-ys-p) = series("exp2_fanout", "processes")
#let (fan-xs-i, fan-ys-i) = series("exp2_fanout", "interpreters")

#solo(
  [Fan-out: many small tasks],
  [
    Wall time per round vs workers. Both mechanisms scale similarly; interpreters
    are modestly ahead at every count, most visibly at 8 workers.
  ],
  xlabel: [workers],
  ylabel: [seconds per round],
  xaxis: (ticks: worker-ticks, subticks: none),
  lq.plot(fan-xs-p, fan-ys-p, mark: "s", label: [processes]),
  lq.plot(fan-xs-i, fan-ys-i, mark: "o", label: [interpreters]),
)

fan-out is essentially a tie; interpreters are consistently a little ahead,
with the gap only noticeable at 8 workers.

== Coordinator: one owner hands out IDs

Workers do a chunk of work, then ask a single owner for the next ID -- the shape
of pygrametl's shared-sequence ID generation. Every request and reply crosses a
queue, and by default an interpreter queue polls for an item only every 10 ms, so
request/reply coordination is very expensive for subinterpreters. Disabling that
poll (`interpreters_no_wait`) brings them level with processes -- confirming the
gap is the poll delay, not the coordination itself. The y-axis is logarithmic so
the small values stay legible next to the default interpreter series.

#let (cr-x-p, cr-y-p) = series("exp4_coordinator", "processes")
#let (cr-x-i, cr-y-i) = series("exp4_coordinator", "interpreters")
#let (cr-x-n, cr-y-n) = series("exp4_coordinator", "interpreters_no_wait")

#solo(
  [Coordinator: 1000 IDs per round],
  [
    Wall time per round on a log axis. The default interpreter queue is 15-30x
    slower, depending on worker count; disabling the 10 ms poll removes the gap.
  ],
  xlabel: [workers],
  ylabel: [seconds per round (log)],
  yscale: "log",
  xaxis: (ticks: worker-ticks, subticks: none),
  lq.plot(cr-x-p, cr-y-p, mark: "s", label: [processes]),
  lq.plot(cr-x-i, cr-y-i, mark: "o", label: [interpreters (default)]),
  lq.plot(cr-x-n, cr-y-n, mark: "^", label: [interpreters (no wait)]),
)

the default interpreter queue makes request/reply coordination 15-30x
more expensive than processes because it polls only every 10 ms; disabling that
poll brings interpreters level with processes, so the penalty is the queue's
poll delay, not coordination itself.

= Data transfer

How fast a value moves from the producer to a worker. The magnitudes differ by
orders of magnitude, so the payloads are split across two charts (small values,
medium containers) to keep the differences visible.

#let transport-throughput(transport, kind, size) = {
  recs("exp1_transport")
    .filter(r => r.transport == transport and r.kind == kind and r.size == size)
    .first()
    .net_mb_per_s
}

#let transport-diagram(title, caption, payloads) = {
  let xs = range(payloads.len())
  let labels = payloads.map(p => p.at(0) + " " + p.at(1))
  half(
    title,
    caption,
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

#grid(
  columns: (1fr, 1fr),
  gutter: 0.4cm,
  transport-diagram(
    [Small values],
    [
      Throughput for tiny immutable values. Interpreters.Queue wins here at
      ~1.5x the process transports.
    ],
    (("int", "small"), ("str", "small"), ("bytes", "small")),
  ),
  transport-diagram(
    [Medium containers],
    [
      Throughput for medium-sized containers. The process transports pull ahead,
      roughly 2-3x the interpreter queue.
    ],
    (("tuple", "medium"), ("list", "medium"), ("dict", "medium")),
  ),
)

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

#solo(
  [Dict transfer: rows/s vs columns],
  [
    Row throughput as the row dict widens. The interpreter queue is consistently
    about half the process rate, at every column count.
  ],
  xlabel: [columns],
  ylabel: [rows/s],
  lq.plot(dict-columns, dict-rows-per-second("interp_queue"), mark: "o", label: [interpreters.Queue]),
  lq.plot(dict-columns, dict-rows-per-second("mp_queue"), mark: "s", label: [mp.Queue]),
  lq.plot(dict-columns, dict-rows-per-second("mp_pipe"), mark: "^", label: [mp.Pipe]),
)

the winner depends on the payload. Interpreters are faster for tiny
immutable values, but for row dicts the process transports are about twice as
fast at every size.

= What the results say

- *CPU scaling:* both reach the same speedup, within run-to-run noise.
  Interpreters use about twice the memory.
- *Startup:* multiprocessing wins clearly; forking a worker is far cheaper than
  creating an interpreter (~16x at 8 workers).
- *Shared read-only buffer:* a tie at two or more workers; both share it at
  the same time and memory cost (about 140 MB at 8 workers).
- *Fan-out:* essentially a tie; interpreters are consistently a little ahead,
  but the margin is small.
- *Coordinator:* the default interpreter queue makes request/reply coordination
  15-30x more expensive than processes because it polls only every 10 ms; disabling
  that poll brings interpreters level with processes, so the penalty is the
  queue's poll delay, not coordination itself.
- *Data transfer:* the winner depends on the payload. Interpreters are faster for
  tiny immutable values, but for row dicts the process transports are about twice
  as fast at every size.

Net: for pygrametl's shape -- rows as dicts, work fanned across queues, IDs from a
shared owner -- multiprocessing is the better fit. It wins startup (~16x),
row-dict transfer (~2x), and coordination (15-30x). Subinterpreters only pull
ahead on tiny immutable values, which are not what an ETL pipeline moves.
CPU-bound work, fan-out, and sharing a large read-only buffer are a wash.
Porting pygrametl to subinterpreters would not pay off: it wins on none of the
axes that define the workload, and loses badly on the ones it does exercise.

= Memory at a glance

Resident memory (MB) per experiment and mechanism, median across runs. Cells
with no data are left blank. Delete this section freely if it feels redundant.

#let mem-cell(name, group, field) = {
  let rs = recs(name).filter(r => r.mechanism == group)
  if rs.len() == 0 { [--] } else {
    let vals = rs.map(r => r.at(field))
    str(calc.round(vals.fold(0, (a, b) => a + b) / vals.len(), digits: 1))
  }
}

#let mem-table(rows) = {
  table(
    columns: (auto, 1fr, auto, auto),
    align: (left, left, right, right),
    stroke: 0.4pt + luma(70%),
    inset: 5pt,
    table.header([*Experiment*], [*Mechanism*], [*avg MB*], [*peak MB*]),
    ..rows,
  )
}

#mem-table((
  [CPU scaling],
  [processes],
  mem-cell("exp5_cpu_memory", "processes", "mem_avg"),
  mem-cell("exp5_cpu_memory", "processes", "mem_max"),
  [CPU scaling],
  [interpreters],
  mem-cell("exp5_cpu_memory", "interpreters", "mem_avg"),
  mem-cell("exp5_cpu_memory", "interpreters", "mem_max"),
  [Fan-out],
  [processes],
  mem-cell("exp2_fanout", "processes", "mem_avg"),
  mem-cell("exp2_fanout", "processes", "mem_max"),
  [Fan-out],
  [interpreters],
  mem-cell("exp2_fanout", "interpreters", "mem_avg"),
  mem-cell("exp2_fanout", "interpreters", "mem_max"),
  [Shared buffer],
  [interpreters (shared)],
  mem-cell("exp6_shared_buffer", "interpreters_shared", "mem_avg"),
  mem-cell("exp6_shared_buffer", "interpreters_shared", "mem_max"),
  [Shared buffer],
  [processes (shared_memory)],
  mem-cell("exp6_shared_buffer", "processes_sharedmem", "mem_avg"),
  mem-cell("exp6_shared_buffer", "processes_sharedmem", "mem_max"),
  [Coordinator],
  [processes],
  mem-cell("exp4_coordinator", "processes", "mem_avg"),
  mem-cell("exp4_coordinator", "processes", "mem_max"),
  [Coordinator],
  [interpreters],
  mem-cell("exp4_coordinator", "interpreters", "mem_avg"),
  mem-cell("exp4_coordinator", "interpreters", "mem_max"),
  [Coordinator],
  [interpreters (no wait)],
  mem-cell("exp4_coordinator", "interpreters_no_wait", "mem_avg"),
  mem-cell("exp4_coordinator", "interpreters_no_wait", "mem_max"),
))

= Raw Results

#raw(read("results/summary.txt"), lang: "text", block: true)
