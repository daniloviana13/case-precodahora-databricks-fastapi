"""
Precodahora Fuel Prices - Scraper Case (Test Mode, Databricks-friendly)

What this script does
- Establishes a session with https://precodahora.ba.gov.br/produtos/
- Extracts the signed CSRF token required for POST requests (X-CSRFToken)
- Fetches ONLY 1 page per fuel (GASOLINA, ETANOL, GNV, DIESEL) for a "case/demo"
- Writes output as JSON Lines (.jsonl), one record per result item, suitable for Databricks:
    spark.read.json(".../data.jsonl")
- Writes a manifest.json per fuel and an overall_manifest.json for auditability
- Produces structured logs (console + file) to validate the run and troubleshoot issues

Install
  pip install requests

Run
  python scraping_price.py

Outputs
  raw_out/
    source=precodahora/anp=GASOLINA/dt=YYYY-MM-DD/run_id=<uuid>/data.jsonl
    source=precodahora/anp=GASOLINA/dt=YYYY-MM-DD/run_id=<uuid>/manifest.json
    ...
  logs/precodahora/<timestamp>.log

Notes
- This endpoint enforces request limits (HTTP 429).
  The script implements exponential backoff + jitter and honors Retry-After when present.
- For a production pipeline, you would:
  (1) schedule the extraction (e.g., every 10 minutes),
  (2) store raw data in a Bronze layer (Delta),
  (3) transform to Silver/Gold models,
  (4) expose via API or dashboards.
"""

import os
import re
import json
import uuid
import time
import random
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin
from typing import Dict, Any, Optional, List, Tuple

import requests

# -----------------------------
# Configuration (case/demo)
# -----------------------------
BASE_URL = "https://precodahora.ba.gov.br/produtos/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")

FUELS = ["GASOLINA", "ETANOL", "GNV", "DIESEL"]

HORAS = 72
LATITUDE = -12.97111
LONGITUDE = -38.51083
RAIO = 100
ORDENAR = "preco.asc"

MAX_PAGES_PER_FUEL = 1  # <-- requested: only 1 page per fuel

# Rate-limit / retry behavior
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0
MAX_BACKOFF_SECONDS = 60.0

# Polite pacing between fuels (still helps avoid 429)
SLEEP_BETWEEN_FUELS = (1.2, 2.4)

OUT_DIR = os.environ.get("OUT_DIR", "./scraping/raw_out")
LOG_DIR = os.environ.get("LOG_DIR", "./scraping/logs/precodahora")

SIGNED_TOKEN_RE = re.compile(r"(Im[A-Za-z0-9_\-]+)\.([A-Za-z0-9_\-]+)\.([A-Za-z0-9_\-]+)")


# -----------------------------
# Logging
# -----------------------------
def setup_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOG_DIR, f"run_{ts}.log")

    logger = logging.getLogger("precodahora")
    logger.setLevel(logging.INFO)

    # clear old handlers
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("Starting run. Log file: %s", log_path)
    return logger


logger = setup_logger()


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def local_date_yyyy_mm_dd() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def safe_slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-=\.]", "_", str(s))


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def append_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# -----------------------------
# CSRF discovery
# -----------------------------
def extract_csrf_from_html(html: str) -> Optional[str]:
    patterns = [
        r'<input[^>]+name=["\']csrf_token["\'][^>]+value=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']',
        r'x-csrftoken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'csrf[_-]?token["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        r'csrfToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
    ]
    for p in patterns:
        m = re.search(p, html, flags=re.IGNORECASE)
        if m:
            return m.group(1)

    m = SIGNED_TOKEN_RE.search(html)
    if m:
        return m.group(0)

    return None


def find_script_srcs(html: str) -> List[str]:
    srcs = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    urls = [urljoin(BASE_URL, s) for s in srcs]

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_csrf_from_js_text(text: str) -> Optional[str]:
    m = SIGNED_TOKEN_RE.search(text)
    return m.group(0) if m else None


def bootstrap_and_find_csrf(session: requests.Session) -> str:
    r = session.get(BASE_URL, timeout=30)
    logger.info("BOOTSTRAP GET %s -> %s", BASE_URL, r.status_code)
    r.raise_for_status()

    # Save bootstrap HTML (debug)
    with open("./scraping/debug_bootstrap.html", "w", encoding="utf-8") as f:
        f.write(r.text)

    csrf = extract_csrf_from_html(r.text)
    if csrf:
        return csrf

    # fallback: scan JS bundles (cap to avoid too much traffic)
    script_urls = find_script_srcs(r.text)
    logger.info("CSRF not found in HTML. Scanning %d script bundles...", len(script_urls))

    headers = {"User-Agent": UA, "Referer": BASE_URL, "Accept": "*/*"}

    for url in script_urls[:25]:
        rr = session.get(url, headers=headers, timeout=30)
        if rr.status_code != 200:
            continue
        token = extract_csrf_from_js_text(rr.text)
        if token:
            logger.info("CSRF found in JS bundle: %s", url)
            return token
        time.sleep(0.10)

    raise RuntimeError("CSRF token not found in HTML or JS bundles.")


# -----------------------------
# HTTP with retry/backoff
# -----------------------------
def compute_backoff(attempt: int) -> float:
    # exponential backoff + jitter
    base = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    jitter = random.uniform(0.25, 0.75) * base
    return min(MAX_BACKOFF_SECONDS, base + jitter)


def request_with_retry(
    method: str,
    session: requests.Session,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 30
) -> requests.Response:
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method, url, headers=headers, data=data, timeout=timeout)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_s = float(retry_after)
                else:
                    wait_s = compute_backoff(attempt)

                logger.warning("429 rate limit. attempt %d/%d. waiting %.1fs", attempt, MAX_RETRIES, wait_s)
                time.sleep(wait_s)
                continue

            # If unauthorized, don't endlessly retry unless it might be transient
            if r.status_code == 401:
                snippet = (r.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"401 unauthorized. snippet={snippet}")

            # Raise for other errors (4xx/5xx)
            r.raise_for_status()
            return r

        except Exception as e:
            last_err = e
            wait_s = compute_backoff(attempt)
            logger.warning("Request error (%s). attempt %d/%d. waiting %.1fs", e, attempt, MAX_RETRIES, wait_s)
            time.sleep(wait_s)

    raise RuntimeError(f"Request failed after {MAX_RETRIES} retries. last_error={last_err}")


def post_products(session: requests.Session, csrf: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://precodahora.ba.gov.br",
        "Referer": BASE_URL,
        "X-CSRFToken": csrf,
    }

    r = request_with_retry("POST", session, BASE_URL, headers=headers, data=payload, timeout=30)

    # Parse JSON safely
    try:
        return r.json()
    except Exception:
        snippet = (r.text or "")[:500].replace("\n", " ")
        raise RuntimeError(f"Response is not JSON. snippet={snippet}")


# -----------------------------
# Output layout (Databricks-friendly)
# -----------------------------
def make_out_paths(base_out: str, anp: str, run_id: str) -> Dict[str, str]:
    dt = local_date_yyyy_mm_dd()
    root = os.path.join(
        base_out,
        "source=precodahora",
        f"anp={safe_slug(anp)}",
        f"dt={dt}",
        f"run_id={run_id}",
    )
    ensure_dir(root)
    return {
        "root": root,
        "data_jsonl": os.path.join(root, "data.jsonl"),
        "manifest": os.path.join(root, "manifest.json"),
    }


def page_to_rows(
    collected_at_utc: str,
    run_id: str,
    anp: str,
    query_meta: Dict[str, Any],
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows = []
    results = data.get("resultado") or []

    for item in results:
        rows.append({
            "collected_at_utc": collected_at_utc,
            "run_id": run_id,
            "source": "precodahora",
            "anp": anp,
            "query": query_meta,
            "raw": item,
        })
    return rows


def collect_one_page_per_fuel(
    session: requests.Session,
    csrf: str,
    base_out: str,
    anp: str,
) -> Dict[str, Any]:
    run_id = str(uuid.uuid4())
    paths = make_out_paths(base_out, anp, run_id)

    collected_at = utc_now_iso()

    # overwrite file for safety
    if os.path.exists(paths["data_jsonl"]):
        os.remove(paths["data_jsonl"])

    payload = {
        "horas": str(HORAS),
        "anp": anp,
        "latitude": str(LATITUDE),
        "longitude": str(LONGITUDE),
        "raio": str(RAIO),
        "pagina": "1",              # only page 1
        "ordenar": ORDENAR,
    }

    logger.info("%s: fetching page 1 (only) ...", anp)
    data = post_products(session, csrf, payload)

    # sanity checks
    total_pages = int(data.get("totalPaginas", 1))
    total_registros = int(data.get("totalRegistros", 0))
    page_count = int(data.get("registrosdaPagina", len(data.get("resultado") or [])))

    results = data.get("resultado") or []
    if not isinstance(results, list):
        raise RuntimeError("Unexpected response format: 'resultado' is not a list.")

    rows = page_to_rows(
        collected_at_utc=collected_at,
        run_id=run_id,
        anp=anp,
        query_meta={
            "horas": HORAS,
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "raio": RAIO,
            "ordenar": ORDENAR,
            "pagina": 1,
        },
        data=data,
    )

    append_jsonl(paths["data_jsonl"], rows)

    manifest = {
        "run_id": run_id,
        "collected_at_utc": collected_at,
        "anp": anp,
        "base_url": BASE_URL,
        "out_root": paths["root"],
        "data_file": os.path.basename(paths["data_jsonl"]),
        "query": {
            "horas": HORAS,
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "raio": RAIO,
            "ordenar": ORDENAR,
            "pagina": 1,
        },
        "response_stats": {
            "totalPaginas_reported": total_pages,
            "totalRegistros_reported": total_registros,
            "registrosdaPagina_reported": page_count,
            "rows_written": len(rows),
        },
    }

    with open(paths["manifest"], "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    logger.info("%s: wrote %d rows (page 1) -> %s", anp, len(rows), paths["data_jsonl"])
    return manifest


def main():
    ensure_dir(OUT_DIR)

    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    })

    csrf = bootstrap_and_find_csrf(session)
    logger.info("Cookies received: %s", list(session.cookies.keys()))
    logger.info("CSRF token found: %s", "yes" if csrf else "no")

    overall = {
        "finished_at_utc": utc_now_iso(),
        "out_dir": os.path.abspath(OUT_DIR),
        "runs": [],
    }

    for idx, anp in enumerate(FUELS, start=1):
        try:
            manifest = collect_one_page_per_fuel(session, csrf, OUT_DIR, anp)
            overall["runs"].append(manifest)
        finally:
            # polite delay between fuels to reduce 429 risk
            if idx < len(FUELS):
                sleep_s = random.uniform(*SLEEP_BETWEEN_FUELS)
                logger.info("Sleeping %.2fs before next fuel...", sleep_s)
                time.sleep(sleep_s)

    overall_path = os.path.join(OUT_DIR, "overall_manifest.json")
    with open(overall_path, "w", encoding="utf-8") as f:
        json.dump(overall, f, ensure_ascii=False, indent=2)

    logger.info("Overall manifest saved: %s", overall_path)
    logger.info("Run completed successfully.")


if __name__ == "__main__":
    main()
