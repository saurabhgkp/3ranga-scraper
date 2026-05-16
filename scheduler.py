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

BACKEND_URL   = os.getenv("BACKEND_URL",   "http://localhost:4000")
INGEST_SECRET = os.getenv("INGEST_SECRET", "internal-scraper-secret")
INTERVAL_MINUTES  = int(os.getenv("SCRAPE_INTERVAL_MINUTES",  "30"))
RESULTS_PER_SITE  = int(os.getenv("SCRAPE_RESULTS_PER_SITE",  "50"))

# ── Search matrix ──────────────────────────────────────────────────────────────
# Every (term, location) pair is scraped independently so we cover both
# role-specific and city-specific results without blowing up API limits.

SEARCH_TERMS = [
    # Engineering roles
    "software engineer",
    "backend developer",
    "frontend developer",
    "full stack developer",
    "software developer",
    "web developer",
    # Specialisations
    "react developer",
    "node.js developer",
    "python developer",
    "java developer",
    "golang developer",
    "devops engineer",
    "cloud engineer",
    "data engineer",
    "data scientist",
    "machine learning engineer",
    "android developer",
    "ios developer",
    "mobile developer",
    # Senior / leadership
    "senior software engineer",
    "tech lead",
    "engineering manager",
    # Adjacent tech
    "product manager",
    "ui ux designer",
    "QA engineer",
    "cybersecurity engineer",
    "blockchain developer",
]

# Top Indian tech-hiring cities + broader "India" catch-all
LOCATIONS = [
    "Bangalore, India",
    "Mumbai, India",
    "Hyderabad, India",
    "Delhi, India",
    "Pune, India",
    "Chennai, India",
    "Noida, India",
    "Gurgaon, India",
    "India",          # catch remote / nationally posted roles
]

_scraper = ScraperService()
_status: dict = {"lastRun": None, "lastCount": 0, "totalPairs": 0, "errors": []}


def _ingest(jobs: list) -> int:
    """POST a batch of jobs to the backend. Returns inserted count."""
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
    """Full scrape pass across all (term, location) pairs."""
    logger.info("[scheduler] Scrape job started — %d terms × %d locations",
                len(SEARCH_TERMS), len(LOCATIONS))

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
    logger.info("Scheduler started — every %d min, %d term × %d location pairs",
                INTERVAL_MINUTES, len(SEARCH_TERMS), len(LOCATIONS))
    return scheduler
