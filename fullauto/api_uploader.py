from __future__ import annotations

import argparse
import base64
import getpass
import hashlib
import json
import mimetypes
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlparse

import requests

try:  # the site sits behind Cloudflare - a browser TLS fingerprint passes it
    from curl_cffi.requests import Session as BrowserSession
except ImportError:
    BrowserSession = None


DEFAULT_SITE = "https://sky.vidfiles.site"
HASH_CHUNK_SIZE = 8 * 1024 * 1024
TRANSFER_CHUNK_SIZE = 1024 * 1024
ALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".jpe", ".png", ".gif", ".webp", ".heic", ".heif", ".avif", ".tif", ".tiff", ".bmp", ".svg",
    ".arw", ".cr2", ".crw", ".dcr", ".erf", ".k25", ".kdc", ".mrw", ".nef", ".orf", ".pef", ".raf", ".raw", ".sr2", ".srf", ".x3f",
    ".mp4", ".m4v", ".mov", ".avi", ".mkv", ".webm", ".3gp", ".mts", ".m2ts", ".wmv", ".mpeg", ".mpg",
}


class UploadError(RuntimeError):
    pass


@dataclass(frozen=True)
class PreparedFile:
    path: Path
    name: str
    size: int
    last_modified_ms: int
    sha1_b64: str


def format_bytes(value: int) -> str:
    size = float(value)
    units = ("B", "KB", "MB", "GB", "TB")
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024
    return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"


def show_progress(label: str, completed: int, total: int, *, finish: bool = False) -> None:
    percent = min(100, int(completed / total * 100)) if total else 100
    width = 28
    filled = int(width * percent / 100)
    bar = "#" * filled + "-" * (width - filled)
    text = f"\r{label:<10} [{bar}] {percent:3d}%  {format_bytes(completed)} / {format_bytes(total)}"
    sys.stdout.write(text + ("\n" if finish else ""))
    sys.stdout.flush()


class ProgressReader:
    def __init__(self, file: BinaryIO, size: int) -> None:
        self.file = file
        self.size = size
        self.completed = 0
        self.last_update = 0.0

    def __len__(self) -> int:
        return self.size

    def read(self, amount: int = -1) -> bytes:
        if amount < 0 or amount > TRANSFER_CHUNK_SIZE:
            amount = TRANSFER_CHUNK_SIZE
        data = self.file.read(amount)
        self.completed += len(data)
        now = time.monotonic()
        if now - self.last_update >= 0.15 or not data or self.completed >= self.size:
            self.last_update = now
            show_progress("Uploading", self.completed, self.size, finish=not data or self.completed >= self.size)
        return data

    def __getattr__(self, name: str) -> Any:
        return getattr(self.file, name)


class VidFilesApi:
    def __init__(self, site: str, api_key: str, timeout: float = 45.0) -> None:
        self.site = normalize_site(site)
        self.timeout = timeout
        self.headers = {"X-API-Key": api_key, "Accept": "application/json", "User-Agent": "VidFiles-API-Uploader/1.0"}
        if BrowserSession is not None:
            self.session = BrowserSession(impersonate="chrome")
        else:
            print("Note: curl_cffi not installed - Cloudflare may block API calls. Fix: pip install curl_cffi")
            self.session = requests.Session()
            self.session.headers.update(self.headers)

    @staticmethod
    def response_detail(response: Any) -> str:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        detail = payload.get("detail") if isinstance(payload, dict) else None
        return str(detail or f"HTTP {response.status_code}")[:240]

    def json_request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("headers", self.headers)
        try:
            response = self.session.request(method, self.site + path, timeout=self.timeout, **kwargs)
        except Exception as exc:
            raise UploadError(f"Could not reach {self.site}: {exc}") from exc
        if response.status_code >= 400:
            raise UploadError(self.response_detail(response))
        try:
            payload = response.json()
        except ValueError as exc:
            raise UploadError("The site returned an invalid response") from exc
        if not isinstance(payload, dict):
            raise UploadError("The site returned an invalid response")
        return payload

    def verify_key(self) -> None:
        self.json_request("GET", "/api/v1/files?limit=1")

    def start(self, item: PreparedFile) -> dict[str, Any]:
        return self.json_request(
            "POST",
            "/api/v1/upload/start",
            json={
                "files": [
                    {
                        "name": item.name,
                        "size": item.size,
                        "last_modified": item.last_modified_ms,
                        "sha1": item.sha1_b64,
                    }
                ]
            },
        )

    def finish(self, job_id: str) -> None:
        self.json_request("POST", f"/api/v1/upload/{job_id}/finish")

    def job(self, job_id: str) -> dict[str, Any]:
        return self.json_request("GET", f"/api/v1/jobs/{job_id}")


def normalize_site(value: str) -> str:
    site = value.strip().rstrip("/") or DEFAULT_SITE
    parsed = urlparse(site)
    local_hosts = {"127.0.0.1", "localhost", "::1"}
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise UploadError("Site must be a complete http:// or https:// URL")
    if parsed.scheme != "https" and parsed.hostname not in local_hosts:
        raise UploadError("Use HTTPS for a public uploader site")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise UploadError("Enter only the site origin, for example https://sky.vidfiles.site")
    return site


def clean_input_path(value: str) -> Path:
    raw = value.strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
        raw = raw[1:-1]
    return Path(raw).expanduser().resolve()


def discover_files(target: Path, recursive: bool) -> tuple[list[Path], list[Path]]:
    if target.is_file():
        candidates = [target]
    elif target.is_dir():
        iterator = target.rglob("*") if recursive else target.glob("*")
        candidates = [path for path in iterator if path.is_file() and not path.is_symlink()]
    else:
        raise UploadError("The selected file or directory does not exist")
    supported = sorted((path for path in candidates if path.suffix.lower() in ALLOWED_EXTENSIONS), key=lambda path: str(path).casefold())
    unsupported = sorted((path for path in candidates if path.suffix.lower() not in ALLOWED_EXTENSIONS), key=lambda path: str(path).casefold())
    return supported, unsupported


def prepare_file(path: Path) -> PreparedFile:
    size = path.stat().st_size
    if size <= 0:
        raise UploadError("Empty files cannot be uploaded")
    digest = hashlib.sha1()
    completed = 0
    with path.open("rb") as source:
        while chunk := source.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
            completed += len(chunk)
            show_progress("Checking", completed, size)
    show_progress("Checking", size, size, finish=True)
    stat = path.stat()
    return PreparedFile(
        path=path,
        name=path.name,
        size=size,
        last_modified_ms=int(stat.st_mtime * 1000),
        sha1_b64=base64.b64encode(digest.digest()).decode("ascii"),
    )


def validate_transfer_url(value: Any) -> str:
    if not isinstance(value, str):
        raise UploadError("The site returned no direct transfer URL")
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.hostname != "photos.googleapis.com" or not parsed.path.startswith("/data/upload/uploadmedia/"):
        raise UploadError("The site returned an unexpected transfer destination")
    return value


def direct_upload(item: PreparedFile, upload_url: str, attempts: int = 3) -> None:
    content_type = mimetypes.guess_type(item.name)[0] or "application/octet-stream"
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        if attempt > 1:
            print(f"Retrying direct transfer ({attempt}/{attempts})...")
            time.sleep(min(2 ** (attempt - 1), 5))
        try:
            with item.path.open("rb") as source:
                reader = ProgressReader(source, item.size)
                response = requests.put(
                    upload_url,
                    headers={"Content-Type": content_type, "Content-Length": str(item.size), "User-Agent": "VidFiles-API-Uploader/1.0"},
                    data=reader,
                    timeout=(30, 180),
                    allow_redirects=False,
                )
            if 200 <= response.status_code < 300:
                return
            last_error = UploadError(f"Direct storage transfer returned HTTP {response.status_code}")
        except requests.RequestException as exc:
            last_error = exc
    raise UploadError(f"Direct transfer failed: {last_error}")


def wait_for_job(api: VidFilesApi, job_id: str, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    previous_stage = ""
    while time.monotonic() < deadline:
        job = api.job(job_id)
        stage = str(job.get("stage") or job.get("status") or "working")
        if stage != previous_stage:
            print(f"Site: {stage.replace('_', ' ').title()}")
            previous_stage = stage
        if job.get("status") == "complete":
            return job
        if job.get("status") == "error":
            raise UploadError(str(job.get("error") or "Site finalization failed"))
        time.sleep(0.8)
    raise UploadError(f"Timed out waiting for job {job_id}")


def save_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def upload_one(api: VidFilesApi, path: Path, wait_timeout: int) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc).isoformat()
    item = prepare_file(path)
    started = api.start(item)
    job_id = str(started.get("job_id") or "")
    transfers = started.get("transfers")
    if not job_id or not isinstance(transfers, list) or len(transfers) != 1 or not isinstance(transfers[0], dict):
        raise UploadError("The site returned invalid transfer metadata")
    transfer = transfers[0]
    deduplicated = bool(transfer.get("skip_transfer"))
    if deduplicated:
        print("Transfer: Existing media matched; no bytes need to be resent.")
    else:
        direct_upload(item, validate_transfer_url(transfer.get("upload_url")))
    api.finish(job_id)
    job = wait_for_job(api, job_id, wait_timeout)
    results = job.get("results") if isinstance(job.get("results"), list) else []
    result = results[0] if results and isinstance(results[0], dict) else {}
    link = result.get("skydrop_url")
    error = result.get("error")
    return {
        "source": str(path),
        "filename": item.name,
        "size_bytes": item.size,
        "sha1_base64": item.sha1_b64,
        "job_id": job_id,
        "site_file_id": result.get("file_id"),
        "deduplicated": deduplicated,
        "status": "ready" if link and not error else "failed",
        "download_url": link,
        "error": error,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload a file or directory through the VidFiles direct-transfer API.")
    parser.add_argument("path", nargs="?", help="File or directory to upload")
    parser.add_argument("--site", default=os.getenv("VIDFILES_SITE", DEFAULT_SITE), help=f"Uploader site (default: {DEFAULT_SITE})")
    parser.add_argument("--api-key", default=os.getenv("VIDFILES_API_KEY", ""), help="API key; prefer VIDFILES_API_KEY or the secure prompt")
    parser.add_argument("--no-recursive", action="store_true", help="Do not scan subdirectories")
    parser.add_argument("--output", help="JSON report path")
    parser.add_argument("--no-report", action="store_true", help="Do not write a local JSON report")
    parser.add_argument("--wait-timeout", type=int, default=1800, help="Seconds to wait for each final link (default: 1800)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print("VidFiles Direct API Uploader")
    print("Media bytes go directly to storage; the API key is sent only to the uploader site.\n")
    try:
        site = normalize_site(args.site or input(f"Uploader site [{DEFAULT_SITE}]: ") or DEFAULT_SITE)
        api_key = args.api_key.strip() or getpass.getpass("API key: ").strip()
        if not api_key:
            raise UploadError("An API key is required")
        target_text = args.path or input("File or directory path: ")
        target = clean_input_path(target_text)
        files, unsupported = discover_files(target, not args.no_recursive)
        if not files:
            raise UploadError("No supported photos or videos were found")
        print(f"Checking API access at {site}...")
        api = VidFilesApi(site, api_key)
        api.verify_key()
        print(f"API working. Found {len(files)} supported file(s).")
        if unsupported:
            print(f"Skipping {len(unsupported)} unsupported file(s).")

        report_path = Path(args.output).expanduser().resolve() if args.output else Path.cwd() / f"vidfiles-upload-report-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
        report: dict[str, Any] = {
            "version": 1,
            "site": site,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "target": str(target),
            "files": [],
        }
        ready = 0
        for index, path in enumerate(files, 1):
            print(f"\n[{index}/{len(files)}] {path.name} ({format_bytes(path.stat().st_size)})")
            try:
                result = upload_one(api, path, max(60, args.wait_timeout))
                if result["status"] == "ready":
                    ready += 1
                    print(f"Ready: {result['download_url']}")
                else:
                    print(f"Failed: {result.get('error') or 'No download link was returned'}")
            except (OSError, UploadError) as exc:
                result = {
                    "source": str(path),
                    "filename": path.name,
                    "size_bytes": path.stat().st_size if path.exists() else None,
                    "status": "failed",
                    "download_url": None,
                    "error": str(exc)[:300],
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                }
                print(f"Failed: {result['error']}")
            report["files"].append(result)
            if not args.no_report:
                save_report(report_path, report)

        print(f"\nFinished: {ready}/{len(files)} link(s) ready.")
        print("The site has saved each upload under the API key owner.")
        if not args.no_report:
            print(f"Local report: {report_path}")
        return 0 if ready == len(files) else 2
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        return 130
    except (OSError, UploadError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
