#!/usr/bin/env python3
"""
Nkiri full-auto agent — watches thenkiri.com and keeps both sites updated.

One script, keeps running:
  1. Lists new thenkiri posts via the WordPress REST API (since last check)
  2. For each new post: scrapes the page → clean name, kind, all episode/movie
     download links → extracts direct URLs from the link protector (downloadwella)
  3. Downloads the videos (clean names) into the download folder
  4. Uploads each file to VidFiles (sha1 dedup — re-runs are cheap)
  5. Sends the post data and VidFiles URLs to the VPS site API
     (movies: TMDB; Korean dramas: MyDramaList; English TV/web series: IMDb)
  6. The VPS API updates its own database and queues a safe background build
  7. Sleeps, repeats. State: state.json — safe to stop/resume anytime.

Usage:
  python nkiri_agent.py --since 2026-09-01      # backfill everything since a date
  python nkiri_agent.py                         # continuous watch (default: since last run)
  python nkiri_agent.py --once --limit 2        # test 2 posts, then exit
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import socket
import subprocess
import sys
import time
import html as html_mod
import shutil
import urllib.parse
import urllib.request
import urllib.error
from http.cookiejar import CookieJar
from pathlib import Path

import api_uploader

SCRIPT_DIR = Path(__file__).resolve().parent
NKIRI_ROOT = SCRIPT_DIR.parent
STATE_PATH = Path(os.environ.get("NKIRI_STATE_FILE") or (SCRIPT_DIR / "state.json"))
SEED_STATE_PATH: Path | None = None
LOG_PATH = SCRIPT_DIR / "agent.log"
DOWNLOAD_DIR = Path(os.environ.get("NKIRI_DOWNLOAD_DIR") or (SCRIPT_DIR / "downloads"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
MIN_FREE_BYTES = int(float(os.environ.get("NKIRI_MIN_FREE_GB", "5")) * 1024 ** 3)
AGENT_LOCK_ADDRESS = ("127.0.0.1", int(os.environ.get("NKIRI_AGENT_LOCK_PORT", "54573")))

THENKIRI = "https://thenkiri.com"
SITE_API = os.environ.get("NKIRI_URL", "https://nkiri.vip").rstrip("/")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36"

ENV = dict(os.environ)
# On the VPS the agent lives outside the public site directory; point it at
# the site's protected environment file rather than duplicating API secrets.
env_file = Path(os.environ.get("NKIRI_ENV_FILE") or (NKIRI_ROOT / ".env"))
if env_file.exists():
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            ENV[k.strip()] = v.strip()
NKIRI_TOKEN = ENV.get("API_TOKEN", "")
TMDB_KEY = ENV.get("TMDB_API_KEY", "")
MDL_BASE = ENV.get("MDL_API_BASE", "https://my-drama-list-api-ten.vercel.app")

def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        # Windows scheduled consoles are often cp1252 even when files are
        # UTF-8; logging must never crash the worker on a checkmark/curly quote.
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        print(line.encode(encoding, errors="replace").decode(encoding), flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")

def cleanup_downloads(files: list[Path]) -> None:
    """Delete local video copies — they live on VidFiles and the site links there."""
    freed = 0
    count = 0
    for p in files:
        try:
            if p.exists():
                freed += p.stat().st_size
                p.unlink()
                count += 1
        except OSError:
            pass
    if count:
        log(f"  🗑 cleaned {count} local file(s), freed {api_uploader.format_bytes(freed)}")

def cleanup_partial_downloads() -> None:
    """Remove resumeless fragments left when the worker is interrupted."""
    removed = 0
    for path in DOWNLOAD_DIR.glob("*.part"):
        try:
            path.unlink()
            removed += 1
        except OSError:
            pass
    if removed:
        log(f"startup cleanup: removed {removed} partial download(s)")

def acquire_agent_lock() -> socket.socket | None:
    """Keep a second click or auto-start from publishing the same post twice."""
    lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        lock.bind(AGENT_LOCK_ADDRESS)
        lock.listen(1)
        return lock
    except OSError:
        lock.close()
        return None

def slugify(text: str) -> str:
    t = text.lower().replace("'", "").replace(":", "")
    return re.sub(r"[^a-z0-9]+", "-", t).strip("-")[:120]

def strip_nulls(obj):
    if isinstance(obj, dict):
        return {k: strip_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [strip_nulls(v) for v in obj if v is not None]
    return obj

def read_state_file(path: Path) -> dict:
    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state, dict):
        raise ValueError("state root is not an object")
    state.setdefault("processed", {})
    state.setdefault("series_links", {})
    return state

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return read_state_file(STATE_PATH)
        except Exception as exc:
            # A previous run was killed while writing state. Preserve the bad
            # file for inspection, then restart with a valid state object.
            damaged = STATE_PATH.with_name(f"state.corrupt-{int(time.time())}.json")
            try:
                STATE_PATH.replace(damaged)
            except OSError:
                pass
            log(f"state recovery: {exc}; starting with empty state")
    if SEED_STATE_PATH and SEED_STATE_PATH.exists() and SEED_STATE_PATH != STATE_PATH:
        try:
            log(f"initializing lane state from {SEED_STATE_PATH.name}")
            return read_state_file(SEED_STATE_PATH)
        except Exception as exc:
            log(f"seed state ignored: {exc}")
    return {"processed": {}, "series_links": {}}

def save_state(state: dict) -> None:
    temporary = STATE_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")
    temporary.replace(STATE_PATH)

def source_signature(wp: dict) -> str:
    """Stable source fingerprint; WP modified catches new episodes/status edits."""
    return "|".join(str(wp.get(k) or "") for k in ("id", "link", "date", "modified", "title"))

def source_time(value) -> str:
    """Normalize WP ISO timestamps and SQLite timestamps for safe comparison."""
    return str(value or "").replace("T", " ").replace("Z", "")[:19]

def http_json(url, payload=None, method="GET", token="", timeout=30):
    headers = {"user-agent": UA, "accept": "application/json"}
    if payload is not None:
        headers["content-type"] = "application/json"
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, method=method,
                                 data=json.dumps(payload).encode() if payload is not None else None,
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return json.loads(res.read().decode("utf-8", "ignore"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")[:400]
        raise RuntimeError(f"HTTP {e.code}: {body}") from e

# ------------------------------------------------------------
# thenkiri: WP REST listing + post page parsing
# ------------------------------------------------------------
def wp_posts(after: str | None, max_pages: int = 80, orderby: str = "date") -> list[dict]:
    out, page = [], 1
    while page <= max_pages:
        url = (f"{THENKIRI}/wp-json/wp/v2/posts?per_page=100&page={page}&orderby={orderby}&order=desc"
               f"&_fields=id,link,date,modified,title"
               + (f"&after={after}T00:00:00" if after else ""))
        batch = None
        for attempt in range(4):
            try:
                req = urllib.request.Request(url, headers={"user-agent": UA, "accept": "application/json"})
                with urllib.request.urlopen(req, timeout=60) as res:
                    batch = json.loads(res.read().decode("utf-8", "ignore"))
                break
            except urllib.error.HTTPError as e:
                if e.code == 400 and page > 1:
                    batch = []
                    break
                if attempt >= 2:
                    log(f"wp list error p{page}: {e}")
                if attempt < 3:
                    time.sleep((2, 5, 12)[attempt])
            except Exception as e:
                if attempt >= 2:
                    log(f"wp list error p{page}: {e}")
                if attempt < 3:
                    time.sleep((2, 5, 12)[attempt])
        if batch is None or not batch:
            break
        out.extend(batch)
        page += 1
    if out:
        return out
    # Fallback: scrape the homepage grid (recent posts, no WP API needed)
    log("wp api unavailable — falling back to homepage scrape")
    try:
        html = get_html(THENKIRI + "/")
        for m in re.finditer(
                r'<a class="eael-grid-post-link" href="(https://thenkiri\.com/[^"]+)"[^>]*title="([^"]+)"', html):
            url, title = m.group(1), html_mod.unescape(m.group(2))
            if not any(p["link"] == url for p in out):
                out.append({"id": 0, "link": url,
                            "title": {"rendered": title}, "content": {"rendered": ""},
                            "date": ""})
    except Exception as e:
        log(f"homepage fallback failed: {e}")
    return out

def parse_post(wp: dict, fetch_page: bool = True) -> dict:
    link = wp["link"]
    raw_title = html_mod.unescape(str(wp["title"]["rendered"]))
    source_type = raw_title.split("|", 1)[-1].strip().lower() if "|" in raw_title else ""
    # The URL alone is ambiguous: korean-tv-series is a drama on the source,
    # while anime-series is a series and Japanese animation is a movie feed.
    if source_type.startswith("download") or " movie" in source_type or "animation" in source_type:
        kind = "movie"
    elif "korean" in source_type or "korean" in link.lower():
        kind = "drama"
    elif "tv series" in source_type or "anime series" in source_type or "series" in source_type:
        kind = "series"
    elif "movie" in link.lower() or "download-" in link.lower():
        kind = "movie"
    else:
        kind = "series"
    content = str((wp.get("content") or {}).get("rendered") or "")

    # clean name / season / year / status
    t = raw_title.split("|")[0].strip()
    year = ""
    ym = re.search(r"\(((?:19|20)\d{2})\)", t)
    if ym:
        year = ym.group(1)
        t = t.replace(ym.group(0), "")
    status = "Complete" if re.search(r"\(Complete\)", t, re.I) else ""
    t = re.sub(r"\((?:Episode[^)]*|Complete)\)", "", t, flags=re.I).strip()
    season = "01"
    sm = re.search(r"\bS(\d+)\b", t, re.I)
    if sm:
        season = f"{int(sm.group(1)):02d}"
        t = re.sub(r"\bS\d+\b", "", t, flags=re.I).strip(" -")
    status = "Ongoing" if not status else status

    # episode/movie links, in page order with their labels
    tokens = re.findall(
        r'<h2[^>]*>([^<]{1,80})</h2>|<a[^>]*class="[^"]*elementor-button[^"]*"[^>]*href="([^"]+)"',
        content, re.S)
    label, links = "", []
    for h2, href in tokens:
        h2s = re.sub(r"\s+", " ", h2).strip() if h2 else ""
        if h2s and re.match(r"(?:Episode|Season|S\d)", h2s, re.I):
            label = h2s
        if href and re.search(r"(downloadwella|\.mkv|\.mp4)", href, re.I):
            links.append((label or "Download", href))
    seen, dedup = set(), []
    for lb, href in links:
        if href not in seen:
            seen.add(href)
            dedup.append((lb, href))
    if not dedup and fetch_page:
        # WP content empty (or API fallback) — pull links from the live page
        try:
            page_html = get_html(link)
            for h2, href in re.findall(
                    r'<h2[^>]*>([^<]{1,80})</h2>.*?<a[^>]*class="[^"]*elementor-button[^"]*"[^>]*href="([^"]+)"',
                    page_html, re.S):
                h2s = re.sub(r"\s+", " ", h2).strip() if h2 else ""
                if h2s and re.match(r"(?:Episode|Season|S\d)", h2s, re.I):
                    label = h2s
                if href and re.search(r"(downloadwella|\.mkv|\.mp4)", href, re.I):
                    if href not in seen:
                        seen.add(href)
                        dedup.append((label or "Download", href))
        except Exception:
            pass
    return {"wp_id": wp["id"], "wp_date": wp.get("date") or "",
            "wp_modified": wp.get("modified") or "", "source_title": raw_title,
            "source_type": source_type, "url": link, "kind": kind,
            "name": t, "season": season, "year": year, "status": status, "links": dedup}

# ------------------------------------------------------------
# downloadwella → direct → download
# ------------------------------------------------------------
def get_html(url: str) -> str:
    cj = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [("user-agent", UA)]
    return opener.open(url, timeout=40).read().decode("utf-8", "ignore")

def downloadwella_direct(page_url: str) -> str | None:
    try:
        cj = CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        opener.addheaders = [("user-agent", UA)]
        html = opener.open(page_url, timeout=40).read().decode("utf-8", "ignore")
        form = re.search(r'<form name="F1".*?</form>', html, re.S)
        if not form:
            return None
        fields = {}
        for inp in re.findall(r"<input[^>]*type=\"hidden\"[^>]*>", form.group(0)):
            n = re.search(r'name="([^"]+)"', inp)
            v = re.search(r'value="([^"]*)"', inp)
            if n:
                fields[n.group(1)] = v.group(1) if v else ""
        wait = 6
        mw = re.search(r"countdown[^0-9]{0,30}(\d+)", html, re.I)
        if mw:
            wait = min(int(mw.group(1)) + 1, 60)
        time.sleep(wait)
        data = urllib.parse.urlencode(fields).encode()
        req2 = urllib.request.Request(page_url, data=data,
                                      headers={"content-type": "application/x-www-form-urlencoded",
                                               "referer": page_url})
        html2 = opener.open(req2, timeout=90).read().decode("utf-8", "ignore")
        m = re.search(r'href="(https?://[a-z0-9.-]+/d/[^"]+)"', html2, re.I)
        return m.group(1) if m else None
    except Exception:
        return None

def direct_url_for(page_url: str) -> str | None:
    if "downloadwella.com" in page_url:
        return downloadwella_direct(page_url)
    try:
        html = get_html(page_url)
        m = re.search(r'href="(https?://[^"]+\.(?:mkv|mp4))"', html, re.I)
        return m.group(1) if m else None
    except Exception:
        return None

def download_file(direct_url: str, dest: Path) -> bool:
    if dest.exists() and dest.stat().st_size > 500:
        return True
    try:
        free = shutil.disk_usage(dest.parent).free
        if free < MIN_FREE_BYTES:
            log(f"  ✗ download paused: only {api_uploader.format_bytes(free)} free (reserve {api_uploader.format_bytes(MIN_FREE_BYTES)})")
            return False
    except OSError:
        return False
    partial = dest.with_name(dest.name + ".part")
    try:
        req = urllib.request.Request(direct_url, headers={"user-agent": UA, "referer": " "})
        with urllib.request.urlopen(req, timeout=90) as res:
            content_length = int(res.headers.get("Content-Length") or 0)
            free = shutil.disk_usage(dest.parent).free
            if content_length and free < MIN_FREE_BYTES + content_length:
                log(f"  ✗ download skipped: {api_uploader.format_bytes(content_length)} file would breach free-space reserve")
                return False
            with open(partial, "wb") as fh:
                while True:
                    chunk = res.read(1024 * 512)
                    if not chunk:
                        break
                    fh.write(chunk)
                    if shutil.disk_usage(dest.parent).free < MIN_FREE_BYTES:
                        raise OSError("free-space reserve reached")
            partial.replace(dest)
        return dest.exists() and dest.stat().st_size > 500
    except Exception:
        partial.unlink(missing_ok=True)
        dest.unlink(missing_ok=True)
        return False

# ------------------------------------------------------------
# metadata: TMDB (movies) / MDL (Korean dramas) / IMDb (English TV/web series)
# ------------------------------------------------------------
def metadata_source(kind: str, source_type: str = "") -> str:
    """Choose the metadata authority from the source classification.

    Korean drama posts are classified as ``drama`` by parse_post.  TV/web
    series (including anime series) are ``series`` and use IMDb.  Movies keep
    their existing TMDB path.
    """
    if kind == "drama":
        return "mydramalist"
    if kind == "series":
        return "imdb"
    if kind == "movie":
        return "tmdb"
    return ""

def tmdb_movie(name: str, year: str) -> dict:
    if not TMDB_KEY:
        return {}
    try:
        q = urllib.parse.quote(name)
        url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={q}"
        if year:
            url += f"&year={year}"
        req = urllib.request.Request(url, headers={"user-agent": UA, "accept": "application/json"})
        d = json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore"))
        results = d.get("results") or []
        if not results:
            return {}
        best = results[0]
        detail = json.loads(urllib.request.urlopen(urllib.request.Request(
            f"https://api.themoviedb.org/3/movie/{best['id']}?api_key={TMDB_KEY}&append_to_response=videos,credits",
            headers={"user-agent": UA, "accept": "application/json"}),
            timeout=20).read().decode("utf-8", "ignore"))
        return detail
    except Exception:
        return {}

def mdl_enrich(name: str) -> dict:
    def mdl_get(path):
        try:
            req = urllib.request.Request(MDL_BASE + path, headers={"user-agent": UA, "accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as res:
                return json.loads(res.read())
        except Exception:
            return None
    def normalize(t):
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", t.lower())).strip()
    search = mdl_get(f"/api/search/q/{urllib.parse.quote(normalize(name))}")
    results = (search or {}).get("results") or []
    if not results:
        return {}
    def score(cand):
        q, c = normalize(name), normalize(cand)
        if c == q:
            return 100
        qw, cw = set(q.split()), set(c.split())
        overlap = len(qw & cw) / max(len(qw), 1)
        # A short title contained in a much longer query (for example
        # "Yakuza" returned for "Yakuza Fiancé: Raise wa Tanin ga Ii") is
        # an ambiguous match, not safe enrichment data.
        if q in c and len(cw) <= len(qw) + 2:
            return 70 + overlap * 10
        if c in q:
            return overlap * 50
        return overlap * 60
    scored = [(score(r.get("title", "")), r) for r in results]
    best_score, best = max(scored, key=lambda item: item[0])
    if best_score < 70:
        return {}
    detail = mdl_get(f"/api/id/{best['slug']}") or {}
    castRes = mdl_get(f"/api/id/{best['slug']}/cast") or {}
    cast = []
    for group, people in (castRes.get("cast") or {}).items():
        for p in (people or [])[:8]:
            cast.append({"name": (p.get("name") or "")[:110],
                         "role": (p.get("character") if group == "Main Role" else group) or None})
    genres = detail.get("genres") if isinstance(detail.get("genres"), list) else []
    return {"rating": str(detail.get("rating")) if detail.get("rating") else None,
            "poster_url": (detail.get("image") or best.get("image")) if str(detail.get("image") or best.get("image") or "").startswith("http") else None,
            "synopsis": detail.get("synopsis") if isinstance(detail.get("synopsis"), str) else None,
            "year": ((re.search(r"\(((?:19|20)\d{2})\)", str(detail.get("title"))).group(1))
                     if re.search(r"\(((?:19|20)\d{2})\)", str(detail.get("title")))
                     else best.get("year")) or None,
            "tags": genres,
            "ext_id": best["slug"], "ext_source": "mydramalist",
            "ext_url": detail.get("url") or f"https://mydramalist.com/{best['slug']}",
            "extra": {"native_title": detail.get("native_title") or None,
                      "network": detail.get("original_network") or None,
                      "aired": detail.get("aired") or None,
                      "duration": detail.get("duration") or None,
                      "content_rating": (detail.get("content_rating") or "").split(" - ")[0] or None,
                      "country": detail.get("country") or None,
                      "episodes_total": detail.get("episodes") if isinstance(detail.get("episodes"), int) else None,
                      "cast": cast[:12] or None}}

def imdb_enrich(name: str, year: str = "") -> dict:
    """Enrich a TV/web series from IMDb's public suggestion and JSON-LD data."""
    def imdb_get(url: str, accept: str) -> str:
        req = urllib.request.Request(url, headers={"user-agent": UA, "accept": accept})
        with urllib.request.urlopen(req, timeout=20) as res:
            return res.read().decode("utf-8", "ignore")

    def imdb_graphql(imdb_id: str) -> dict:
        query = (
            'query { title(id: "' + imdb_id + '") { '
            'id titleText { text } releaseYear { year } '
            'ratingsSummary { aggregateRating voteCount } '
            'primaryImage { url } plot { plotText { plainText } } '
            'titleGenres { genres { genre { text } } } '
            'credits(first: 12) { edges { node { name { nameText { text } } '
            '... on Cast { characters { name } } } } } '
            '} }'
        )
        try:
            req = urllib.request.Request(
                "https://api.graphql.imdb.com/",
                data=json.dumps({"query": query}).encode("utf-8"),
                headers={"user-agent": UA, "accept": "application/json",
                         "content-type": "application/json",
                         "origin": "https://www.imdb.com", "referer": "https://www.imdb.com/"},
            )
            with urllib.request.urlopen(req, timeout=20) as res:
                body = json.loads(res.read().decode("utf-8", "ignore"))
            return ((body.get("data") or {}).get("title") or {})
        except Exception:
            return {}

    def normalize(t: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", str(t or "").lower())).strip()

    try:
        query = urllib.parse.quote(name, safe="")
        search = json.loads(imdb_get(
            f"https://v3.sg.media-imdb.com/suggestion/x/{query}.json",
            "application/json"))
    except Exception:
        return {}

    candidates = []
    for item in (search.get("d") or []):
        imdb_id = str(item.get("id") or "")
        qtype = str(item.get("q") or "").lower()
        if not imdb_id.startswith("tt"):
            continue
        # The suggestion feed also returns movies, names, shorts, and videos.
        # Keep TV/streaming entries so an identically named movie cannot win.
        if qtype and "tv" not in qtype and "series" not in qtype:
            continue
        candidates.append(item)
    if not candidates:
        return {}

    wanted = normalize(name)
    wanted_words = set(wanted.split())

    def score(item: dict) -> float:
        candidate = normalize(item.get("l"))
        if not candidate:
            return -1
        if candidate == wanted:
            value = 100
        else:
            candidate_words = set(candidate.split())
            overlap = len(wanted_words & candidate_words) / max(len(wanted_words), 1)
            if wanted in candidate and len(candidate_words) <= len(wanted_words) + 2:
                value = 78 + overlap * 10
            elif candidate in wanted:
                value = overlap * 55
            else:
                value = overlap * 65
        if str(item.get("q") or "").lower() in {"tv series", "tv mini series"}:
            value += 4
        if year and item.get("y"):
            try:
                if abs(int(item["y"]) - int(year)) > 1:
                    value -= 35
            except (TypeError, ValueError):
                pass
        return value

    best_score, best = max(((score(item), item) for item in candidates), key=lambda pair: pair[0])
    if best_score < 82:
        return {}

    imdb_id = str(best["id"])
    data = imdb_graphql(imdb_id)
    graphql_data = bool(data)
    if not graphql_data:
        # Fallback for environments where the public GraphQL endpoint is
        # unavailable. IMDb may return a bot-check page, so this is optional.
        try:
            page = imdb_get(f"https://www.imdb.com/title/{imdb_id}/", "text/html")
            match = re.search(r'<script type="application/ld\+json">\s*(.*?)\s*</script>', page, re.S)
            if match:
                decoded = json.loads(html_mod.unescape(match.group(1)))
                if isinstance(decoded, list):
                    data = next((item for item in decoded if isinstance(item, dict)
                                 and item.get("@type") in ("TVSeries", "Movie", "TVMiniSeries")), {})
                elif isinstance(decoded, dict):
                    data = decoded
        except Exception:
            data = {}

    def first_image(value):
        if isinstance(value, list):
            return first_image(value[0]) if value else None
        return value if isinstance(value, str) and value.startswith("http") else None

    def first_text(value):
        if isinstance(value, list):
            return ", ".join(str(v) for v in value if v)
        if isinstance(value, dict):
            return str(value.get("name") or "") or None
        return str(value) if value else None

    actors = []
    if graphql_data:
        edges = ((data.get("credits") or {}).get("edges") or []) if isinstance(data, dict) else []
        for edge in edges:
            node = (edge or {}).get("node") or {}
            person = ((node.get("name") or {}).get("nameText") or {}).get("text")
            characters = [c.get("name") for c in (node.get("characters") or []) if c.get("name")]
            if person:
                actors.append({"name": str(person)[:110],
                               "role": ", ".join(characters)[:110] or None})
    else:
        for actor in (data.get("actor") or []) if isinstance(data, dict) else []:
            if isinstance(actor, dict) and actor.get("name"):
                actors.append({"name": str(actor["name"])[:110], "role": None})
    aggregate = (data.get("ratingsSummary") if graphql_data else data.get("aggregateRating")) if isinstance(data, dict) else {}
    if not isinstance(aggregate, dict):
        aggregate = {}
    if graphql_data:
        genres = [((entry.get("genre") or {}).get("text"))
                  for entry in ((data.get("titleGenres") or {}).get("genres") or [])]
    else:
        genres = data.get("genre") if isinstance(data, dict) else []
    if isinstance(genres, str):
        genres = [genres]
    if not isinstance(genres, list):
        genres = []
    published_year = (str(((data.get("releaseYear") or {}).get("year")) or "")[:4]
                      if graphql_data else str(data.get("datePublished") or "")[:4]) if isinstance(data, dict) else ""
    result_year = published_year if re.fullmatch(r"(?:19|20)\d{2}", published_year) else best.get("y")
    poster = (data.get("primaryImage") or {}).get("url") if graphql_data and isinstance(data, dict) else first_image(data.get("image")) if isinstance(data, dict) else None
    poster = poster or first_image((best.get("i") or {}).get("imageUrl"))
    rating_value = aggregate.get("aggregateRating") or aggregate.get("ratingValue")
    votes = aggregate.get("voteCount") if graphql_data else aggregate.get("ratingCount")
    try:
        votes = int(votes) if votes is not None else None
    except (TypeError, ValueError):
        votes = None
    return strip_nulls({
        "rating": str(rating_value) if rating_value else None,
        "poster_url": poster,
        "synopsis": (((data.get("plot") or {}).get("plotText") or {}).get("plainText")
                     if graphql_data else data.get("description")) if isinstance(data, dict) else None,
        "year": result_year,
        "tags": [str(tag) for tag in genres if tag],
        "ext_id": imdb_id,
        "ext_source": "imdb",
        "ext_url": f"https://www.imdb.com/title/{imdb_id}/",
        "extra": {
            "duration": data.get("duration") if isinstance(data, dict) and not graphql_data else None,
            "content_rating": data.get("contentRating") if isinstance(data, dict) and not graphql_data else None,
            "country": first_text(data.get("contentLocation")) if isinstance(data, dict) else None,
            "votes": votes,
            "cast": actors[:12] or None,
        },
    })

# ------------------------------------------------------------
# VidFiles: download → upload → skydrop link
# ------------------------------------------------------------
def vidfiles_api_key() -> str:
    api_key = ENV.get("VIDFILES_API_KEY", "").strip()
    key_file = SCRIPT_DIR / "apikey.txt"
    if not api_key and key_file.exists():
        api_key = key_file.read_text(encoding="utf-8").strip()
    if not api_key:
        raise RuntimeError("VIDFILES_API_KEY is not configured")
    return api_key

def upload_flow(page_link: str, dest: Path, uploader) -> tuple[bool, str | None]:
    direct = direct_url_for(page_link)
    if not direct:
        return False, None
    fname = os.path.basename(urllib.parse.urlparse(direct).path) or dest.name
    dest = dest.with_name(fname)
    if not download_file(direct, dest):
        return False, None
    result = uploader(api_uploader.VidFilesApi(
        api_uploader.normalize_site(os.environ.get("VIDFILES_SITE", api_uploader.DEFAULT_SITE)),
        vidfiles_api_key()), dest, 3600)
    if result.get("status") == "ready" and result.get("download_url"):
        return True, result["download_url"]
    return False, None

# ------------------------------------------------------------
# post processing
# ------------------------------------------------------------
def normalize_name(t: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", t.lower())).strip()

def api_posts_index() -> list[dict] | None:
    """Fetch the lightweight post index once per cycle for safe matching."""
    try:
        posts = []
        for kind in ("movie", "drama", "series"):
            res = http_json(f"{SITE_API}/api/posts?kind={kind}&limit=5000", token=NKIRI_TOKEN)
            posts.extend(res.get("posts") or [])
        return posts
    except Exception as exc:
        log(f"site index unavailable: {exc}")
        return None

def _season_of(post: dict) -> int | None:
    m = re.search(r"\bS(\d+)\b", str(post.get("title") or ""), re.I)
    return int(m.group(1)) if m else None

def api_find(kind: str, name: str, season: str | None = None,
             year: str = "", index: list[dict] | None = None) -> dict | None:
    """Find one post, preferring exact name + season to avoid cross-season merges."""
    try:
        if index is None:
            res = http_json(f"{SITE_API}/api/posts?kind={kind}&limit=5000", token=NKIRI_TOKEN)
            index = res.get("posts") or []
        q = normalize_name(name)
        sn = int(season) if season and str(season).isdigit() else None
        def find_in(pool: list[dict]) -> list[dict]:
            exact = [p for p in pool if normalize_name(p.get("name") or "") == q]
            if not exact:
                exact = [p for p in pool if q and q in normalize_name(p.get("name") or "")]
            if sn is not None:
                # A different season is not a safe match. Returning no match
                # lets the caller try a legacy kind or create the right row.
                exact = [p for p in exact if _season_of(p) == sn]
            if kind == "movie" and year:
                by_year = [p for p in exact if str(p.get("year") or "") == str(year)]
                if by_year:
                    exact = by_year
            return exact

        exact = find_in([p for p in index if p.get("kind") == kind])
        if not exact and kind in {"drama", "series"}:
            # Older imports could classify Korean TV posts as series (or use
            # MDL for English series). Reuse that slug so a correction updates
            # the existing record instead of creating a duplicate.
            exact = find_in([p for p in index if p.get("kind") in {"drama", "series"}])
        return exact[0] if exact else None
    except Exception:
        return None

def source_is_stale(wp: dict, post: dict, index: list[dict] | None) -> bool:
    """Detect updates to a previously completed source post."""
    existing = api_find(post["kind"], post["name"], post["season"], post.get("year") or "", index)
    if not existing:
        return True
    src_modified = source_time(wp.get("modified"))
    local_modified = source_time(existing.get("wp_modified"))
    if src_modified and local_modified:
        return src_modified > local_modified
    return html_mod.unescape(str(wp.get("title", {}).get("rendered") or "")).strip() != str(existing.get("title") or "").strip()

def process_post(post: dict, state: dict, args) -> str:
    kind, name, season = post["kind"], post["name"], post["season"]
    year, status = post["year"], post["status"]
    wp_date = (post.get("wp_date") or "").replace("T", " ")[:19]
    wp_modified = (post.get("wp_modified") or "").replace("T", " ")[:19]
    links = post["links"]
    log(f"processing [{kind}] {name} S{season} — {len(links)} link(s)")
    if not links:
        log("  no download links found")
        return "unavailable"
    if getattr(args, "dry_run", False):
        log(f"  dry-run: parser found {len(links)} link(s); no download/upload/site write")
        return "pending"
    if args.no_upload:
        log("  no-upload requested — no site write performed")
        return "pending"
    if args.no_download:
        log("  no-download cannot be combined with uploading — no site write performed")
        return "pending"

    uploader = api_uploader.VidFilesApi(
        api_uploader.normalize_site(os.environ.get("VIDFILES_SITE", api_uploader.DEFAULT_SITE)),
        vidfiles_api_key())

    # download + upload every linked file; collect skydrop links in page order
    skydrops = []
    downloaded_files: list[Path] = []
    all_links_ok = True
    for label, page_link in links:
        state_key = page_link
        prev = state["series_links"].get(state_key)
        if prev:
            skydrops.append((label, state_key, prev))
            continue
        direct = direct_url_for(page_link)
        if not direct:
            log(f"  ✗ no direct URL: {page_link[:70]}")
            all_links_ok = False
            continue
        fname = os.path.basename(urllib.parse.urlparse(direct).path) or f"{slugify(name)}.mkv"
        dest = DOWNLOAD_DIR / fname
        was_present = dest.exists() and dest.stat().st_size > 500
        log(f"  ↓ downloading {fname}…")
        if not download_file(direct, dest):
            log("  ✗ download failed")
            all_links_ok = False
            continue
        if not was_present:
            downloaded_files.append(dest)
        try:
            result = api_uploader.upload_one(uploader, dest, 7200)
        except Exception as e:
            result = {"status": "error", "error": str(e)}
        if result.get("status") == "ready" and result.get("download_url"):
            skydrops.append((label, page_link, result["download_url"]))
            state["series_links"][state_key] = result["download_url"]
            save_state(state)
            log(f"  ✓ uploaded → {result['download_url'][:70]}")
        else:
            log(f"  ✗ upload failed: {result.get('error')}")
            all_links_ok = False

    if not skydrops:
        log("  nothing uploaded — post skipped")
        return "unavailable"

    # build the site payload
    episodes = []
    multi = len({m.group(1).upper() for _, pl, _ in skydrops
                 if (m := re.search(r"S(\d+)", pl, re.I))}) > 1
    for label, page_link, url in skydrops:
        # prefer the SxxExx embedded in the downloadwella page URL (the
        # filename always carries it), then the heading label
        fm = re.search(r"S(\d+)E(\d+)", page_link, re.I)
        if fm:
            snum, num = int(fm.group(1)), int(fm.group(2))
        else:
            em = re.search(r"E(\d+)", label, re.I)
            num = int(em.group(1)) if em else 0
            sm2 = re.search(r"S(\d+)", label, re.I)
            snum = int(sm2.group(1)) if sm2 else int(season)
        episodes.append({"number": num, "label": label, "url": url,
                         "_season": snum, "_multi": multi})
    episodes.sort(key=lambda e: (e["_season"], e["number"]))
    for e in episodes:
        e["label"] = (f"S{e['_season']:02d}E{e['number']:02d}" if e["_multi"]
                      else f"Episode {e['number']:02d}")

    source_title = post.get("source_title") or ""
    kind_label = "Korean Drama" if kind == "drama" else "TV Series"
    base = {"kind": kind, "name": name, "status": status, "year": year or None,
            "published": True, "auto_fill": True,
            "links": [{"label": "Download Movie", "url": skydrops[0][2]}]}

    if kind == "movie":
        tm = tmdb_movie(name, year)
        # multi-part movies: every part gets its own download button
        if len(skydrops) > 1:
            base["links"] = [{"label": f"Download Part {i + 1}", "url": u}
                             for i, (_, _, u) in enumerate(skydrops)]
        base.update({
            "title": source_title or f"{name} ({year}) | Download Hollywood Movie",
            "synopsis": tm.get("overview"), "poster_url":
                f"https://image.tmdb.org/t/p/w342{tm['poster_path']}" if tm.get("poster_path") else None,
            "rating": str(round(tm["vote_average"], 1)) if tm.get("vote_average") else None,
            "runtime": f"{tm['runtime']} min" if tm.get("runtime") else None,
            "imdb_id": tm.get("imdb_id"),
            "ext_id": str(tm.get("id") or ""),
            "ext_source": "tmdb",
            "ext_url": (f"https://www.themoviedb.org/movie/{tm['id']}" if tm.get("id") else None),
            "tags": [g["name"] for g in tm.get("genres", [])],
            "trailer_url": next((f"https://www.youtube.com/embed/{v['key']}"
                                 for v in (tm.get("videos", {}).get("results") or [])
                                 if v.get("site") == "YouTube" and "Trailer" in v.get("type", "")), None),
            "extra": {"tagline": tm.get("tagline"), "votes": tm.get("vote_count"),
                      "country": ", ".join(tm.get("origin_country") or []) or None,
                      "cast": [{"name": c["name"][:110], "role": (c.get("character") or "")[:110] or None}
                               for c in (tm.get("credits", {}).get("cast") or [])[:10]] or None},
        })
        payload = strip_nulls({k: v for k, v in base.items() if v is not None})
        existing_movie = api_find("movie", name, year=year, index=getattr(args, "_api_index", None))
        if existing_movie:
            payload["slug"] = existing_movie["slug"]
        payload["wp_date"] = wp_date or None
        payload["wp_modified"] = wp_modified or None
        res = http_json(f"{SITE_API}/api/posts", payload, method="POST", token=NKIRI_TOKEN)
        log(f"  ✓ site: {res['post']['slug']}")
        cleanup_downloads(downloaded_files)
        return "done" if all_links_ok else "partial"

    # drama / series
    existing = api_find(kind, name, season=season, index=getattr(args, "_api_index", None))
    existing_full = None
    if existing:
        try:
            existing_full = http_json(f"{SITE_API}/api/posts/{existing['slug']}",
                                      token=NKIRI_TOKEN)["post"]
        except Exception:
            existing_full = None

    # merge episodes: existing site episodes + newly uploaded ones (by label)
    merged_eps: dict[str, dict] = {}
    if existing_full:
        sm = re.search(r"S(\d+)", existing_full.get("title") or "", re.I)
        base_season = int(sm.group(1)) if sm else 1
        for e in existing_full.get("episodes") or []:
            lbl_season = re.search(r"S(\d+)", e["label"], re.I)
            merged_eps[e["label"]] = {
                "number": e["number"], "label": e["label"], "url": e["url"],
                "_season": int(lbl_season.group(1)) if lbl_season else base_season}
    for e in episodes:
        merged_eps[e["label"]] = e
    episodes = sorted(merged_eps.values(), key=lambda e: (e["_season"], e["number"]))
    multi = any(e.get("_multi") for e in episodes)
    for e in episodes:
        e["label"] = (f"S{e['_season']:02d}E{e['number']:02d}" if multi
                      else f"Episode {e['number']:02d}")

    source = metadata_source(kind, post.get("source_type") or "")
    meta = mdl_enrich(name) if source == "mydramalist" else imdb_enrich(name, year) if source == "imdb" else {}
    status_text = ("Complete" if status == "Complete"
                   else f"Episode {max((e['number'] for e in episodes), default=0)} Added")
    title = f"{name} S{int(season):02d} ({status_text}) | {kind_label}"
    slug = slugify(f"{name} s{int(season):02d} {status_text.lower()} {kind_label.lower().replace(' ', '-')}")
    payload = strip_nulls({
        "title": source_title or title, "kind": kind, "name": name, "status": status,
        "year": year or (meta.get("year") if meta else None),
        "synopsis": (meta or {}).get("synopsis"),
        "poster_url": (meta or {}).get("poster_url"),
        "rating": (meta or {}).get("rating"),
        "ext_id": (meta or {}).get("ext_id"),
        "ext_source": (meta or {}).get("ext_source"),
        "ext_url": (meta or {}).get("ext_url"),
        "tags": (meta or {}).get("tags") or [],
        "extra": (meta or {}).get("extra") or {},
        "episodes": [{"number": e["number"], "label": e["label"], "url": e["url"]}
                     for e in episodes],
        "published": True,
        "auto_fill": True,
        "wp_date": wp_date or None,
        "wp_modified": wp_modified or None,
    }) or {}
    payload["slug"] = existing["slug"] if existing else slugify(source_title or title)
    res = http_json(f"{SITE_API}/api/posts", payload, method="POST", token=NKIRI_TOKEN)
    log(f"  ✓ site: {res['post']['slug']} ({'updated' if existing else 'new'})")
    cleanup_downloads(downloaded_files)
    return "done" if all_links_ok else "partial"

# ------------------------------------------------------------
# main
# ------------------------------------------------------------
def queue_posts(path: str) -> list[dict]:
    """Turn an audit CSV row into the small WP-shaped object the parser needs."""
    out = []
    with open(path, encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            link = (row.get("source_url") or "").strip()
            title = (row.get("title") or row.get("name") or "").strip()
            if not link or not title:
                continue
            out.append({
                "id": row.get("source_id") or 0,
                "link": link,
                "date": row.get("published_date") or "",
                "modified": row.get("modified_date") or row.get("published_date") or "",
                "title": {"rendered": title},
                "content": {"rendered": ""},
            })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-09-01", help="backfill posts from this date")
    ap.add_argument("--interval", type=int, default=900, help="watch interval seconds")
    ap.add_argument("--limit", type=int, default=0, help="max posts per cycle (0 = all)")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--no-download", action="store_true")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="parse source posts only; never download, upload, or write the site")
    ap.add_argument("--no-sync", action="store_true", help="deprecated; retained for old launch commands")
    ap.add_argument("--queue-file", help="historical audit CSV; process only these source URLs")
    ap.add_argument("--state-file", help="state file for this independent worker lane")
    ap.add_argument("--seed-state-file", help="copy this existing state into a lane the first time it runs")
    ap.add_argument("--shard-index", type=int, default=0, help="zero-based queue shard assigned to this lane")
    ap.add_argument("--shard-count", type=int, default=1, help="number of stable queue shards")
    args = ap.parse_args()

    if args.shard_count < 1 or args.shard_index < 0 or args.shard_index >= args.shard_count:
        ap.error("--shard-index must be within --shard-count")

    def resolve_worker_path(value: str) -> Path:
        # Command-line paths are relative to the shell's working directory,
        # matching Python's normal CLI behaviour. The default state remains
        # beside this script when no path is supplied.
        return Path(value).expanduser()

    global STATE_PATH, SEED_STATE_PATH
    if args.state_file:
        STATE_PATH = resolve_worker_path(args.state_file)
    if args.seed_state_file:
        SEED_STATE_PATH = resolve_worker_path(args.seed_state_file)
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

    agent_lock = acquire_agent_lock()
    if agent_lock is None:
        log("another local agent is already running; this copy will exit")
        return 0

    state = load_state()
    cleanup_partial_downloads()
    log("agent started")

    def process_batch(posts: list[dict], label: str) -> bool:
        api_index = None
        index_loaded = False
        fresh = []
        for wp in posts:
            p = parse_post(wp, fetch_page=False)
            key = wp["link"]
            prev = state["processed"].get(key)
            if prev == "skipped (wwe)":
                continue
            if prev == "done":
                if not index_loaded:
                    api_index = api_posts_index()
                    index_loaded = True
                # Legacy state entries are checked against the site's stored
                # WP modified time so old posts can receive later episodes.
                if api_index is None or not source_is_stale(wp, p, api_index):
                    continue
            elif isinstance(prev, dict) and prev.get("status") == "done":
                if prev.get("signature") == source_signature(wp):
                    continue
            elif isinstance(prev, dict) and prev.get("status") in {
                "blocked", "partial", "skipped_unavailable", "skipped_error"
            }:
                continue
            elif isinstance(prev, dict) and prev.get("status") == "pending":
                attempts = int(prev.get("attempts") or 0)
                if args.queue_file and attempts >= 3:
                    if not args.dry_run:
                        state["processed"][key] = {
                            **prev,
                            "status": "skipped_unavailable",
                            "reason": "source failed historical retries",
                            "skipped_at": int(time.time()),
                        }
                        save_state(state)
                    log(f"  queue: unavailable after historical retries: {p['name']}")
                    continue
                last = float(prev.get("last_attempt") or 0)
                if time.time() - last < max(60, args.interval):
                    continue
            fresh.append((wp, p))
        if args.limit:
            fresh = fresh[:args.limit]
        log(f"{label}: {len(posts)} thenkiri posts, {len(fresh)} to process")
        changed = False
        for wp, p in fresh:
            if not p["links"]:
                p = parse_post(wp, fetch_page=True)
            if "wwe" in p["name"].lower():
                state["processed"][wp["link"]] = "skipped (wwe)"
                save_state(state)
                continue
            args._api_index = api_index
            try:
                result = process_post(p, state, args)
                if args.dry_run or args.no_upload:
                    continue
                previous = state["processed"].get(wp["link"])
                attempts = int(previous.get("attempts") or 0) if isinstance(previous, dict) else 0
                if result == "done":
                    state["processed"][wp["link"]] = {
                        "status": "done", "signature": source_signature(wp),
                        "modified": wp.get("modified") or "", "completed_at": int(time.time()),
                    }
                    changed = True
                elif args.queue_file and result == "partial":
                    # A post with some uploaded files is useful on the site, but
                    # permanently unavailable source links must not keep it in a
                    # retry loop during a historical backfill.
                    state["processed"][wp["link"]] = {
                        "status": "partial", "signature": source_signature(wp),
                        "modified": wp.get("modified") or "", "completed_at": int(time.time()),
                        "reason": "one or more source links were unavailable",
                    }
                    changed = True
                elif args.queue_file and result == "unavailable":
                    # Historical queues get one real attempt per source post.
                    # Dead hosts, blocked downloads, and unresolvable links are
                    # recorded separately and never consume another runner slot.
                    state["processed"][wp["link"]] = {
                        "status": "skipped_unavailable",
                        "signature": source_signature(wp),
                        "modified": wp.get("modified") or "", "skipped_at": int(time.time()),
                        "reason": "no usable downloadable source link",
                    }
                else:
                    state["processed"][wp["link"]] = {
                        "status": "pending", "signature": source_signature(wp),
                        "attempts": attempts + 1, "last_attempt": time.time(),
                    }
                    changed = changed or result == "partial"
                save_state(state)
            except Exception as e:
                log(f"  ✗ {p['name']}: {e}")
                if not args.dry_run and not args.no_upload:
                    if args.queue_file:
                        state["processed"][wp["link"]] = {
                            "status": "skipped_error", "signature": source_signature(wp),
                            "modified": wp.get("modified") or "", "skipped_at": int(time.time()),
                            "reason": "worker could not process source post",
                        }
                    else:
                        previous = state["processed"].get(wp["link"])
                        attempts = int(previous.get("attempts") or 0) if isinstance(previous, dict) else 0
                        state["processed"][wp["link"]] = {
                            "status": "pending", "signature": source_signature(wp),
                            "attempts": attempts + 1,
                            "last_attempt": time.time(),
                        }
                    save_state(state)
        if changed and not args.dry_run and not args.no_upload:
            # Each post was already published directly to the VPS API.  The API
            # owns the live database and queues a build without stopping the site.
            log("site updates sent through the VPS API; no database sync or service restart")
        return changed

    def cycle():
        if args.queue_file:
            queue = queue_posts(args.queue_file)
            if args.shard_count > 1:
                queue = [post for position, post in enumerate(queue)
                         if position % args.shard_count == args.shard_index]
                label = (f"queue {Path(args.queue_file).name} "
                         f"shard {args.shard_index + 1}/{args.shard_count}")
            else:
                label = f"queue {Path(args.queue_file).name}"
            process_batch(queue, label)
            return
        # Keep the normal watch window bounded, while also checking the most
        # recently modified records so an old series can receive new episodes
        # without accidentally starting a 7,000-post historical import.
        recent = wp_posts(args.since)
        revised = wp_posts(None, max_pages=1, orderby="modified")
        merged = {p["link"]: p for p in recent}
        merged.update({p["link"]: p for p in revised})
        posts = list(merged.values())
        process_batch(posts, "cycle")

    if args.once:
        cycle()
        return 0

    # first pass: backfill everything since --since, then loop
    backlog = wp_posts(args.since)
    process_batch(backlog, f"backfill since {args.since}")

    while True:
        time.sleep(args.interval)
        try:
            cycle()
        except Exception as e:
            log(f"cycle error: {e}")

if __name__ == "__main__":
    main()
