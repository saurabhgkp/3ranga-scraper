"""
APScheduler — triggers scrape every SCRAPE_INTERVAL_MINUTES and POSTs
results to the backend ingest endpoint.
"""

import logging
import os

import httpx
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from scraper_service import ScraperService

logger = logging.getLogger(__name__)

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:4000")
INGEST_SECRET = os.getenv("INGEST_SECRET", "internal-scraper-secret")
INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "30"))
RESULTS_PER_SITE = int(os.getenv("SCRAPE_RESULTS_PER_SITE", "25"))

SEARCH_TERMS = [
    "software engineer",
    "backend developer",
    "frontend developer",
    "full stack developer",
    "data scientist",
    "machine learning engineer",
    "devops engineer",
    "product manager",
]

_scraper = ScraperService()
_status: dict = {"lastRun": None, "lastCount": 0, "errors": []}


def run_scrape_job() -> None:
    """Single scrape iteration — called by scheduler and manual trigger."""
    logger.info("[scheduler] Scrape job started")
    total_sent = 0
    errors = []

    for term in SEARCH_TERMS:
        try:
            jobs = _scraper.scrape(
                search_term=term,
                location="United States",
                results_per_site=RESULTS_PER_SITE,
            )
            if not jobs:
                continue

            resp = httpx.post(
                f"{BACKEND_URL}/api/jobs/ingest",
                json={"jobs": jobs},
                headers={"x-scraper-secret": INGEST_SECRET},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            inserted = data.get("inserted", 0)
            total_sent += inserted
            logger.info("  '%s' → %d inserted", term, inserted)

        except Exception as exc:
            msg = f"{term}: {exc}"
            errors.append(msg)
            logger.error("  Error for '%s': %s", term, exc)

    import datetime
    _status["lastRun"] = datetime.datetime.utcnow().isoformat()
    _status["lastCount"] = total_sent
    _status["errors"] = errors[-5:]  # keep last 5
    logger.info("[scheduler] Done — %d jobs inserted total", total_sent)


def get_status() -> dict:
    return _status


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_job,
        trigger=IntervalTrigger(minutes=INTERVAL_MINUTES),
        id="scrape_job",
        name="JobSpy scraper",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    logger.info("Scheduler started — every %d min", INTERVAL_MINUTES)
    return scheduler
