"""Run four independent Nkiri workers on one GitHub-hosted runner.

Each GitHub job owns four non-overlapping queue shards.  The child workers
have separate state files, download folders, and lock ports, so a runner can
use its resources in parallel without allowing one worker to overwrite
another worker's progress.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path


RUNNER_COUNT = 4


@dataclass(frozen=True)
class Lane:
    label: str
    queue_file: str
    state_file: str
    seed_state_file: str
    shard_index: int
    shard_count: int
    post_limit: int
    start_delay: int


def lanes_for_runner(runner_index: int) -> list[Lane]:
    if runner_index < 0 or runner_index >= RUNNER_COUNT:
        raise ValueError(f"runner index must be between 0 and {RUNNER_COUNT - 1}")

    movie_start = runner_index * 2
    return [
        Lane(
            label=f"movies shard {movie_start + 1}/8",
            queue_file="queue/missing-movies.csv",
            state_file=f"fullauto/state-movies-shard-{movie_start}.json",
            seed_state_file="fullauto/state-movies-a.json",
            shard_index=movie_start,
            shard_count=8,
            post_limit=8,
            start_delay=0,
        ),
        Lane(
            label=f"movies shard {movie_start + 2}/8",
            queue_file="queue/missing-movies.csv",
            state_file=f"fullauto/state-movies-shard-{movie_start + 1}.json",
            seed_state_file="fullauto/state-movies-b.json",
            shard_index=movie_start + 1,
            shard_count=8,
            post_limit=8,
            start_delay=15,
        ),
        Lane(
            label=f"English/other series shard {runner_index + 1}/4",
            queue_file="queue/missing-series.csv",
            state_file=f"fullauto/state-english-series-shard-{runner_index}.json",
            seed_state_file="fullauto/state-english-series.json",
            shard_index=runner_index,
            shard_count=4,
            post_limit=2,
            start_delay=30,
        ),
        Lane(
            label=f"Korean drama shard {runner_index + 1}/4",
            queue_file="queue/missing-korean-dramas.csv",
            state_file=f"fullauto/state-korean-dramas-shard-{runner_index}.json",
            seed_state_file="fullauto/state-korean-dramas.json",
            shard_index=runner_index,
            shard_count=4,
            post_limit=2,
            start_delay=45,
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-index", type=int, required=True)
    args = parser.parse_args()

    lanes = lanes_for_runner(args.runner_index)
    children: list[subprocess.Popen[str]] = []
    children_lock = threading.Lock()
    results: dict[str, int] = {}
    results_lock = threading.Lock()

    def stop_children(signum, _frame):
        print(f"received signal {signum}; stopping child workers", flush=True)
        with children_lock:
            for child in children:
                if child.poll() is None:
                    child.terminate()

    signal.signal(signal.SIGTERM, stop_children)
    signal.signal(signal.SIGINT, stop_children)

    runner_temp = Path(os.environ.get("RUNNER_TEMP") or tempfile.gettempdir())
    download_root = runner_temp / "nkiri-downloads"

    def run_lane(position: int, lane: Lane) -> None:
        if lane.start_delay:
            time.sleep(lane.start_delay)

        lane_id = f"runner-{args.runner_index}-worker-{position}"
        env = os.environ.copy()
        env["NKIRI_DOWNLOAD_DIR"] = str(download_root / lane_id)
        env["NKIRI_AGENT_LOCK_PORT"] = str(54573 + args.runner_index * 10 + position + 1)
        env["PYTHONUNBUFFERED"] = "1"

        command = [
            sys.executable,
            "fullauto/nkiri_agent.py",
            "--once",
            "--queue-file",
            lane.queue_file,
            "--state-file",
            lane.state_file,
            "--seed-state-file",
            lane.seed_state_file,
            "--shard-index",
            str(lane.shard_index),
            "--shard-count",
            str(lane.shard_count),
            "--limit",
            str(lane.post_limit),
            "--no-sync",
        ]

        child = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        with children_lock:
            children.append(child)
        print(f"[{lane.label}] started ({lane_id})", flush=True)

        assert child.stdout is not None
        for line in child.stdout:
            print(f"[{lane.label}] {line.rstrip()}", flush=True)
        code = child.wait()
        with results_lock:
            results[lane.label] = code
        print(f"[{lane.label}] exited with code {code}", flush=True)

    threads = [
        threading.Thread(target=run_lane, args=(position, lane), daemon=False)
        for position, lane in enumerate(lanes)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    print("worker summary:", flush=True)
    for lane in lanes:
        print(f"- {lane.label}: exit {results.get(lane.label, 'unknown')}", flush=True)
    return 0 if all(code == 0 for code in results.values()) and len(results) == len(lanes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
