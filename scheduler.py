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

from scraper_service import ScraperService

logger = logging.getLogger(__name__)

BACKEND_URL      = os.getenv("BACKEND_URL",              "http://localhost:4000")
INGEST_SECRET    = os.getenv("INGEST_SECRET",            "internal-scraper-secret")
INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "240"))
RESULTS_PER_SITE = int(os.getenv("SCRAPE_RESULTS_PER_SITE", "15"))

# ── Focused search matrix ──────────────────────────────────────────────────────
# Kept intentionally small so linkedin_fetch_description=True (which makes one
# extra HTTP request per job) stays under ~1,000 description fetches per run.
# 10 terms × 6 locations × 15 results × 3 sites = 2,700 raw → ~800–1,200 unique

SEARCH_TERMS = [
    "software engineer",
    "backend developer",
    "frontend developer",
    "full stack developer",
    "react developer",
    "python developer",
    "devops engineer",
    "data scientist",
    "machine learning engineer",
    "mobile developer",
]

LOCATIONS = [
    "Bangalore, India",
    "Mumbai, India",
    "Hyderabad, India",
    "Delhi, India",
    "Pune, India",
    "India",           # catch remote / nationally posted roles
]

_scraper = ScraperService()
_status: dict = {"lastRun": None, "lastCount": 0, "totalPairs": 0, "errors": []}


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
    logger.info("[scheduler] Scrape started — %d terms × %d locations × %d results/site",
                len(SEARCH_TERMS), len(LOCATIONS), RESULTS_PER_SITE)

    total_inserted = 0
    errors: list[str] = []
    pairs_done = 0

    for location in LOCATIONS:
        for term in SEARCH_TERMS:
            try:
                jobs = _scraper.scrape(
                    search_term=term,
                    location=location,
                    results_per_site=RESULTS_PER_SITE,
                    country_indeed="India",
                )
                inserted = _ingest(jobs)
                total_inserted += inserted
                pairs_done += 1
                logger.info("  [%s / %s] scraped %d → inserted %d",
                            term, location, len(jobs), inserted)
            except Exception as exc:
                msg = f"{term} @ {location}: {exc}"
                errors.append(msg)
                logger.error("  Error — %s", msg)

    _status["lastRun"]    = datetime.datetime.utcnow().isoformat()
    _status["lastCount"]  = total_inserted
    _status["totalPairs"] = pairs_done
    _status["errors"]     = errors[-10:]
    logger.info("[scheduler] Done — %d jobs inserted across %d pairs",
                total_inserted, pairs_done)


def get_status() -> dict:
    return _status


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_job,
        trigger=IntervalTrigger(minutes=INTERVAL_MINUTES),
        id="scrape_job",
        name="JobSpy India scraper",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    logger.info("Scheduler started — every %d min", INTERVAL_MINUTES)
    return scheduler
