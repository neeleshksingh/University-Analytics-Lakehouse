import argparse
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

from src.lakehouse.config import OPS

RUN_LOG = Path(OPS) / "run_log.jsonl"

STEPS = [
    ("extract", ["-m", "src.ingestion.extract"]),
    ("bronze",  ["-m", "src.lakehouse.bronze"]),
    ("silver",  ["-m", "src.lakehouse.silver"]),
    ("gold",    ["-m", "src.lakehouse.gold"]),
    ("export",  ["-m", "src.lakehouse.export"]),
]


def log(entry):
    """Append one line of JSON to the run log."""
    RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
    with RUN_LOG.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def run_step(args):
    """Run one step as its own process. Returns (exit_code, seconds)."""
    start = time.time()
    result = subprocess.run([sys.executable, *args])
    return result.returncode, round(time.time() - start, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delta-days", type=int, default=0)
    opts = parser.parse_args()

    steps = list(STEPS)
    if opts.delta_days:
        steps.insert(0, ("generate", ["-m", "src.generator", "delta", "--days", str(opts.delta_days)]))

    run_id = uuid.uuid4().hex[:12]
    print(f"Pipeline run {run_id}: {' -> '.join(name for name, _ in steps)}")

    for name, args in steps:
        print(f"\n=== {name} ===")
        code, seconds = run_step(args)

        log({
            "run_id": run_id,
            "step": name,
            "status": "success" if code == 0 else "failed",
            "exit_code": code,
            "seconds": seconds,
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        })

        if code != 0:
            print(f"=== {name} FAILED (exit code {code}) ===")
            sys.exit(code)

        print(f"=== {name} ok ({seconds}s) ===")

    print(f"\nPipeline run {run_id} succeeded. Log: {RUN_LOG}")


if __name__ == "__main__":
    main()