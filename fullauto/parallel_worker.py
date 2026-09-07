"""Run four independent Nkiri workers on one GitHub-hosted runner.

Each GitHub job owns four non-overlapping queue shards.  The child workers
have separate state files, download folders, and lock ports, so a runner can
use its resources in parallel without allowing one worker to overwrite
another worker's progress.
"""
from __future__ import annotations

import argparse
import json
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
REPO_COUNT = 5
MOVIE_SHARD_COUNT = 40
ENGLISH_SERIES_SHARD_COUNT = 20
KOREAN_DRAMA_SHARD_COUNT = 20

# The original private repository is intentionally not assigned a slot.  The
# five public copies below own disjoint portions of the same historical queue.
REPO_SLOTS = {
    "rajusingh-bit/nkiri-automation": 0,
    "rajiv-pixelupis/nkiri-automation": 1,
    "sumiji-ctrl/nkiri-automation": 2,
    "annuji-arch/nkiri-automation": 3,
    "anildev-rgb/nkiri-automation": 4,
}


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


def repo_slot_from_repository(repository: str | None = None) -> int:
    """Return the five-repository queue slot for this GitHub checkout."""
    value = (repository or os.environ.get("GITHUB_REPOSITORY") or "").strip().lower()
    if value not in REPO_SLOTS:
        known = ", ".join(sorted(REPO_SLOTS))
        raise ValueError(f"unsupported GITHUB_REPOSITORY {value!r}; expected one of {known}")
    return REPO_SLOTS[value]


def _state_path(category: str, shard_index: int) -> str:
    return f"fullauto/state-{category}-global-shard-{shard_index}.json"


def lanes_for_runner(runner_index: int, repo_slot: int) -> list[Lane]:
    if runner_index < 0 or runner_index >= RUNNER_COUNT:
        raise ValueError(f"runner index must be between 0 and {RUNNER_COUNT - 1}")
    if repo_slot < 0 or repo_slot >= REPO_COUNT:
        raise ValueError(f"repo slot must be between 0 and {REPO_COUNT - 1}")

    movie_start = repo_slot * 8 + runner_index * 2
    english_shard = repo_slot * 4 + runner_index
    korean_shard = repo_slot * 4 + runner_index
    return [
        Lane(
            label=f"movies shard {movie_start + 1}/{MOVIE_SHARD_COUNT}",
            queue_file="queue/missing-movies.csv",
            state_file=_state_path("movies", movie_start),
            seed_state_file="fullauto/state-movies-a.json",
            shard_index=movie_start,
            shard_count=MOVIE_SHARD_COUNT,
            post_limit=8,
            start_delay=0,
        ),
        Lane(
            label=f"movies shard {movie_start + 2}/{MOVIE_SHARD_COUNT}",
            queue_file="queue/missing-movies.csv",
            state_file=_state_path("movies", movie_start + 1),
            seed_state_file="fullauto/state-movies-b.json",
            shard_index=movie_start + 1,
            shard_count=MOVIE_SHARD_COUNT,
            post_limit=8,
            start_delay=15,
        ),
        Lane(
            label=f"English/other series shard {english_shard + 1}/{ENGLISH_SERIES_SHARD_COUNT}",
            queue_file="queue/missing-series.csv",
            state_file=_state_path("english-series", english_shard),
            seed_state_file="fullauto/state-english-series.json",
            shard_index=english_shard,
            shard_count=ENGLISH_SERIES_SHARD_COUNT,
            post_limit=2,
            start_delay=30,
        ),
        Lane(
            label=f"Korean drama shard {korean_shard + 1}/{KOREAN_DRAMA_SHARD_COUNT}",
            queue_file="queue/missing-korean-dramas.csv",
            state_file=_state_path("korean-dramas", korean_shard),
            seed_state_file="fullauto/state-korean-dramas.json",
            shard_index=korean_shard,
            shard_count=KOREAN_DRAMA_SHARD_COUNT,
            post_limit=2,
            start_delay=45,
        ),
    ]


def all_lanes_for_repo(repo_slot: int) -> list[Lane]:
    return [lane for runner in range(RUNNER_COUNT)
            for lane in lanes_for_runner(runner, repo_slot)]


def _state_status(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return str(value.get("status") or "")
    return ""


def _state_rank(value) -> int:
    status = _state_status(value)
    if status == "done" or status.startswith("skipped"):
        return 4
    if status == "blocked":
        return 3
    if status == "pending":
        return 2
    return 1


def _state_modified(value) -> str:
    if isinstance(value, dict):
        return str(value.get("modified") or "")
    return ""


def _state_score(value) -> tuple[int, int, float, int]:
    attempts = 0
    timestamp = 0.0
    if isinstance(value, dict):
        try:
            attempts = int(value.get("attempts") or 0)
        except (TypeError, ValueError):
            attempts = 0
        for field in ("completed_at", "last_attempt", "blocked_at"):
            try:
                timestamp = max(timestamp, float(value.get(field) or 0))
            except (TypeError, ValueError):
                pass
    # Prefer a structured record over a legacy status string when the status
    # and source version are otherwise identical.
    return (_state_rank(value), attempts, timestamp, 1 if isinstance(value, dict) else 0)


def _merge_processed(target: dict, incoming: dict) -> None:
    for key, candidate in incoming.items():
        if key not in target:
            target[key] = candidate
            continue
        current = target[key]
        current_modified = _state_modified(current)
        candidate_modified = _state_modified(candidate)
        # A later source revision must win even if an older copy was already
        # marked done.  This lets the normal agent update a series when new
        # episodes are added after the audit snapshot.
        if current_modified and candidate_modified and candidate_modified != current_modified:
            if candidate_modified > current_modified:
                target[key] = candidate
            continue
        if _state_score(candidate) > _state_score(current):
            target[key] = candidate


def _read_seed(path: Path) -> dict:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return raw
    except (OSError, ValueError, TypeError):
        return {}


def _seed_candidates(category: str) -> list[Path]:
    root = Path("fullauto")
    if category == "movies":
        patterns = ["state-movies-a.json", "state-movies-b.json", "state-movies-shard-*.json"]
    elif category == "english-series":
        patterns = ["state-english-series.json", "state-english-series-shard-*.json"]
    else:
        patterns = ["state-korean-dramas.json", "state-korean-dramas-shard-*.json"]
    paths = [root / "state.json"]
    for pattern in patterns:
        paths.extend(sorted(root.glob(pattern)))
    return list(dict.fromkeys(path for path in paths if path.exists()))


def initialize_global_state(lane: Lane) -> None:
    """Create a new global lane state by safely merging legacy progress."""
    target = Path(lane.state_file)
    if target.exists():
        return
    category = Path(lane.state_file).name.removeprefix("state-").split("-global-")[0]
    merged = {"processed": {}, "series_links": {}}
    for source_path in _seed_candidates(category):
        source = _read_seed(source_path)
        processed = source.get("processed")
        if isinstance(processed, dict):
            _merge_processed(merged["processed"], processed)
        links = source.get("series_links")
        if isinstance(links, dict):
            for key, value in links.items():
                if value and key not in merged["series_links"]:
                    merged["series_links"][key] = value
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(merged, indent=1, ensure_ascii=False), encoding="utf-8")
    temporary.replace(target)
    print(f"initialized {target} from {len(_seed_candidates(category))} legacy state file(s)", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-index", type=int, required=True)
    parser.add_argument("--repo-slot", type=int,
                        help="five-repository queue slot; defaults to GITHUB_REPOSITORY")
    args = parser.parse_args()

    repo_slot = args.repo_slot if args.repo_slot is not None else repo_slot_from_repository()
    lanes = lanes_for_runner(args.runner_index, repo_slot)
    for lane in lanes:
        initialize_global_state(lane)
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
