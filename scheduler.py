"""
APScheduler — triggers scrape every SCRAPE_INTERVAL_MINUTES and POSTs
results to the backend ingest endpoint.
"""

import datetime
import logging
import os

import httpx
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from jsearch_service import JSearchService

logger = logging.getLogger(__name__)

BACKEND_URL      = os.getenv("BACKEND_URL",              "http://localhost:4000")
INGEST_SECRET    = os.getenv("INGEST_SECRET",            "internal-scraper-secret")
INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "240"))
NUM_PAGES        = int(os.getenv("JSEARCH_PAGES_PER_QUERY",  "1"))   # 10 jobs per page

# ── Search queries ─────────────────────────────────────────────────────────────
# Each query = 1 API call = 10 jobs (NUM_PAGES=1).
# Free tier: 200 req/month → ~6 queries/run at 4-hour interval.
# Paid tiers allow more queries — increase NUM_PAGES or add more QUERIES.

QUERIES = [
    "software engineer jobs in Bangalore India",
    "backend frontend developer jobs in Mumbai India",
    "full stack developer jobs in Hyderabad India",
    "react node python developer jobs in Delhi India",
    "devops cloud engineer jobs in Pune India",
    "data scientist machine learning engineer jobs in India",
    "java golang developer jobs in Bangalore India",
    "mobile android ios developer jobs in India",
    "senior software engineer tech lead jobs in India",
    "product manager ui ux designer tech jobs in India",
]

_service = JSearchService()
_status: dict = {"lastRun": None, "lastCount": 0, "totalQueries": 0, "errors": []}


def _ingest(jobs: list) -> int:
    if not jobs:
        return 0
    resp = httpx.post(
        f"{BACKEND_URL}/api/jobs/ingest",
        json={"jobs": jobs},
        headers={"x-scraper-secret": INGEST_SECRET},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("inserted", 0)


def run_scrape_job() -> None:
    logger.info("[scheduler] JSearch scrape started — %d queries × %d page(s)",
                len(QUERIES), NUM_PAGES)

    total_inserted = 0
    errors: list[str] = []
    queries_done = 0

    for query in QUERIES:
        try:
            jobs = _service.search(query, num_pages=NUM_PAGES)
            inserted = _ingest(jobs)
            total_inserted += inserted
            queries_done += 1
            logger.info("  [%s] → %d inserted", query[:50], inserted)
        except Exception as exc:
            msg = f"{query[:50]}: {exc}"
            errors.append(msg)
            logger.error("  Error — %s", msg)

    _status["lastRun"]      = datetime.datetime.utcnow().isoformat()
    _status["lastCount"]    = total_inserted
    _status["totalQueries"] = queries_done
    _status["errors"]       = errors[-10:]
    logger.info("[scheduler] Done — %d jobs inserted from %d queries",
                total_inserted, queries_done)


def get_status() -> dict:
    return _status


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_job,
        trigger=IntervalTrigger(minutes=INTERVAL_MINUTES),
        id="scrape_job",
        name="JSearch India scraper",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    logger.info("Scheduler started — every %d min, %d queries per run",
                INTERVAL_MINUTES, len(QUERIES))
    return scheduler
