#!/usr/bin/env python3
"""VidFiles V2 direct uploader used by the historic and live workers.

V2 avoids the old whole-file SHA1 pass and transfers up to six complete files
directly to the storage URLs returned by the API.  The module is importable by
the worker and also remains useful as a small command-line uploader.
"""

from __future__ import annotations

import argparse
import base64
import mimetypes
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import monotonic
from typing import Any

import requests


DEFAULT_SITE = "https://sky.vidfiles.site"
MAX_PUT_SECONDS = 7200
USER_AGENT = "Nkiri-VidFiles-V2/1.0"


class V2UploadError(RuntimeError):
    pass


def site_origin(value: str) -> str:
    value = value.strip().rstrip("/")
    if not value.startswith("https://"):
        raise V2UploadError("The V2 uploader requires an HTTPS site origin")
    return value


def describe_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if amount < 1024 or unit == "GiB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{value} B"


class V2Client:
    def __init__(self, site: str, api_key: str, timeout: int = 45) -> None:
        if not api_key:
            raise V2UploadError("VIDFILES_API_KEY is not configured")
        self.site = site_origin(site)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": api_key,
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        })

    def json(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self.session.request(method, self.site + path,
                                        timeout=self.timeout, **kwargs)
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code >= 400:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            raise V2UploadError(
                f"{method} {path} HTTP {response.status_code}: "
                f"{detail or response.text[:240]}"
            )
        if not isinstance(payload, dict):
            raise V2UploadError(f"{method} {path} returned non-object JSON")
        return payload

    def start(self, files: list[Path]) -> dict[str, Any]:
        metadata = []
        for path in files:
            stat = path.stat()
            metadata.append({
                "name": path.name,
                "size": stat.st_size,
                "last_modified": int(stat.st_mtime * 1000),
            })
        return self.json("POST", "/api/v2/upload/start", json={"files": metadata})

    def put_one(self, index: int, path: Path,
                transfer: dict[str, Any]) -> tuple[int, str]:
        if transfer.get("skip_transfer"):
            return index, ""
        upload_url = transfer.get("upload_url")
        if not isinstance(upload_url, str) or not upload_url.startswith("https://"):
            raise V2UploadError(f"file {index} returned no valid direct upload URL")
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        with path.open("rb") as source:
            response = requests.put(
                upload_url,
                headers={
                    "Content-Type": content_type,
                    "Content-Length": str(path.stat().st_size),
                    "User-Agent": USER_AGENT,
                },
                data=source,
                timeout=(30, MAX_PUT_SECONDS),
                allow_redirects=False,
            )
        if not 200 <= response.status_code < 300:
            raise V2UploadError(
                f"file {index} direct PUT HTTP {response.status_code}: "
                f"{response.text[:240]}"
            )
        return index, base64.b64encode(response.content).decode("ascii")

    def finish(self, job_id: str, receipts: list[dict[str, Any]]) -> dict[str, Any]:
        return self.json("POST", f"/api/v2/upload/{job_id}/finish",
                         json={"receipts": receipts})

    def wait(self, job_id: str, deadline: float) -> dict[str, Any]:
        last_stage = ""
        while monotonic() < deadline:
            job = self.json("GET", f"/api/v2/jobs/{job_id}")
            stage = str(job.get("stage") or job.get("status") or "working")
            if stage != last_stage:
                print(f"V2 site: {stage.replace('_', ' ').title()}", flush=True)
                last_stage = stage
            if job.get("status") == "complete":
                return job
            if job.get("status") == "error":
                raise V2UploadError(str(job.get("error") or "V2 finalization failed"))
            time.sleep(1)
        raise V2UploadError(f"timed out waiting for V2 job {job_id}")


def upload_batch(client: V2Client, files: list[Path], batch_size: int = 6,
                 wait_timeout: int = 7200) -> list[dict[str, Any]]:
    """Upload one complete batch and return ready results in file order."""
    if not files:
        return []
    if len(files) > 6:
        raise V2UploadError("V2 batches cannot contain more than six files")
    if not 1 <= batch_size <= 6:
        raise V2UploadError("V2 batch_size must be between one and six")
    for path in files:
        if not path.is_file() or path.stat().st_size <= 0:
            raise V2UploadError(f"file is missing or empty: {path}")

    started = monotonic()
    batch = client.start(files)
    job_id = str(batch.get("job_id") or "")
    transfers = batch.get("transfers")
    if not job_id or not isinstance(transfers, list) or len(transfers) != len(files):
        raise V2UploadError("V2 start returned invalid job or transfer metadata")
    max_parallel = int(batch.get("max_parallel_files") or 6)
    workers = min(max(1, batch_size), max_parallel, len(files))
    print(f"V2 job {job_id}: {len(files)} file(s), max_parallel={max_parallel}, "
          f"workers={workers}", flush=True)

    receipts: list[dict[str, Any] | None] = [None] * len(files)
    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="vidfiles-v2") as pool:
        futures = {
            pool.submit(client.put_one, index, path, transfer): index
            for index, (path, transfer) in enumerate(zip(files, transfers))
        }
        for future in as_completed(futures):
            index, receipt = future.result()
            receipts[index] = {
                "index": index,
                "upload_response_b64": receipt,
            }
            print(f"V2 PUT complete: {files[index].name} "
                  f"({describe_bytes(files[index].stat().st_size)})", flush=True)

    client.finish(job_id, [r for r in receipts if r is not None])
    result = client.wait(job_id, monotonic() + max(60, wait_timeout))
    results = result.get("results") if isinstance(result.get("results"), list) else []
    by_index: dict[int, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        try:
            if item.get("index") is not None:
                by_index[int(item["index"])] = item
        except (TypeError, ValueError):
            pass
        name = item.get("filename") or item.get("name")
        if name:
            by_name[str(name)] = item

    ordered = []
    for index, path in enumerate(files):
        item = by_index.get(index) or by_name.get(path.name) or {}
        if item.get("skydrop_url") and not item.get("download_url"):
            item = {**item, "download_url": item["skydrop_url"]}
        ordered.append(item)
    elapsed = monotonic() - started
    print(f"V2 complete in {elapsed:.1f}s; results={len(results)}", flush=True)
    return ordered


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload files with VidFiles V2")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--site", default=os.getenv("VIDFILES_SITE", DEFAULT_SITE))
    parser.add_argument("--api-key", default=os.getenv("VIDFILES_API_KEY", ""))
    parser.add_argument("--batch-size", type=int, default=6)
    parser.add_argument("--wait-timeout", type=int, default=1800)
    args = parser.parse_args()
    if not args.api_key:
        raise SystemExit("VIDFILES_API_KEY is required")
    files = [path.resolve() for path in args.files]
    if not 1 <= len(files) <= 6:
        raise SystemExit("Provide between one and six files for one V2 batch")
    client = V2Client(args.site, args.api_key)
    upload_batch(client, files, args.batch_size, args.wait_timeout)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, requests.RequestException, V2UploadError) as exc:
        print(f"V2 upload failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
