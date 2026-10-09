"""Run the full experiment suite.

Each experiment runs in its own process and writes its collected result
({experiment, records, summary}) to a temp file whose path is passed as
argv[1]. This module aggregates those into:

  - results/summary.txt   (human-readable tables)
  - results/results.json  (all records, for chart generation)
"""
import json
import os
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
EXPERIMENTS = [
    "exp1_transport",
    "exp2_fanout",
    "exp3_dict_transport",
    "exp4_coordinator",
    "exp5_cpu_memory",
    "exp6_shared_buffer",
    "exp7_startup",
]


def child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([HERE, env.get("PYTHONPATH", "")])
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def run_experiment(name: str) -> dict:
    """Run one experiment in its own process and return its result dict."""
    print(f"Running {name}")
    script = os.path.join(HERE, "experiments", name + ".py")
    file_descriptor, result_path = tempfile.mkstemp(prefix=name + "-",
                                                    suffix=".json")
    os.close(file_descriptor)
    try:
        process = subprocess.run(
            [sys.executable, script, result_path],
            capture_output=True, text=True, env=child_env(), check=False,
        )
        if process.returncode != 0:
            sys.stderr.write(process.stderr)
            raise SystemExit(f"{name} failed (exit {process.returncode})")
        if process.stdout:
            print(process.stdout, end="")
        with open(result_path) as f:
            return json.load(f)
    finally:
        if os.path.exists(result_path):
            os.remove(result_path)


def main() -> None:
    os.makedirs(RESULTS, exist_ok=True)
    header = [
        "Subinterpreters vs multiprocessing - combined summary",
        "=" * 84,
        (
            f"Python {sys.version.split()[0]} | logical CPUs {os.cpu_count()} "
            f"| generated {time.asctime()}"
        ),
        "All times are medians over the raw runs; ranges are min..max.",
    ]
    results = []
    for name in EXPERIMENTS:
        results.append(run_experiment(name))
        records = {result["experiment"]: result["records"] for result in results}
        summary = "\n".join(header + [result["summary"] for result in results]) + "\n"
        with open(os.path.join(RESULTS, "results.json"), "w") as f:
            json.dump(records, f, indent=2)
        with open(os.path.join(RESULTS, "summary.txt"), "w") as f:
            f.write(summary)
        print(f"done {name}", flush=True)

if __name__ == "__main__":
    main()
