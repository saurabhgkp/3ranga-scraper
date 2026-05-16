"""
APScheduler — triggers scrape every SCRAPE_INTERVAL_MINUTES and POSTs
results to the backend ingest endpoint.

Sources:
  1. JSearch (RapidAPI) — full descriptions, structured data
  2. JobSpy (web scraping) — free, additional volume from Indeed/Glassdoor/LinkedIn
Results are deduplicated by titleHash before ingestion.
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
NUM_PAGES        = int(os.getenv("JSEARCH_PAGES_PER_QUERY",  "10"))  # 10 jobs per page, max=10
ENABLE_JOBSPY    = os.getenv("ENABLE_JOBSPY", "true").lower() == "true"

# ── JSearch queries ────────────────────────────────────────────────────────────
JSEARCH_QUERIES = [
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

# ── JobSpy queries (web scraping — free, additional volume) ────────────────────
JOBSPY_QUERIES = [
    {"search_term": "software engineer",         "location": "Bangalore, India"},
    {"search_term": "full stack developer",      "location": "Mumbai, India"},
    {"search_term": "backend developer",         "location": "Hyderabad, India"},
    {"search_term": "frontend developer react",  "location": "Delhi, India"},
    {"search_term": "devops cloud engineer",     "location": "Pune, India"},
    {"search_term": "data scientist",            "location": "Bangalore, India"},
    {"search_term": "java developer",            "location": "India"},
    {"search_term": "python developer",          "location": "India"},
    {"search_term": "react node developer",      "location": "India"},
    {"search_term": "mobile ios android",        "location": "India"},
]

_jsearch_service = None  # initialised lazily on first run
_status: dict = {
    "lastRun": None, "lastCount": 0,
    "jsearchInserted": 0, "jobspyInserted": 0,
    "totalQueries": 0, "errors": [],
}


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
    global _jsearch_service
    logger.info("[scheduler] Scrape started — JSearch (%d queries) + JobSpy (%s)",
                len(JSEARCH_QUERIES), "enabled" if ENABLE_JOBSPY else "disabled")

    errors: list[str] = []
    seen_hashes: set[str] = set()

    # ── Phase 1: JSearch ──────────────────────────────────────────────────────
    jsearch_jobs: list[dict] = []
    jsearch_queries_done = 0

    try:
        if _jsearch_service is None:
            _jsearch_service = JSearchService()
    except RuntimeError as exc:
        logger.error("[scheduler] JSearch disabled — %s", exc)
        errors.append(f"jsearch/init: {exc}")

    for query in JSEARCH_QUERIES if _jsearch_service else []:
        try:
            jobs = _jsearch_service.search(query, num_pages=NUM_PAGES)
            new_jobs = [j for j in jobs if j["titleHash"] not in seen_hashes]
            for j in new_jobs:
                seen_hashes.add(j["titleHash"])
            jsearch_jobs.extend(new_jobs)
            jsearch_queries_done += 1
            logger.info("  [jsearch] '%s' → %d new jobs", query[:50], len(new_jobs))
        except Exception as exc:
            msg = f"jsearch/{query[:40]}: {exc}"
            errors.append(msg)
            logger.error("  %s", msg)

    jsearch_inserted = _ingest(jsearch_jobs)
    logger.info("[scheduler] JSearch done — %d inserted from %d queries",
                jsearch_inserted, jsearch_queries_done)

    # ── Phase 2: JobSpy ───────────────────────────────────────────────────────
    jobspy_inserted = 0

    if ENABLE_JOBSPY:
        try:
            from scraper_service import ScraperService
            scraper = ScraperService()
            jobspy_jobs: list[dict] = []

            for q in JOBSPY_QUERIES:
                try:
                    jobs = scraper.scrape(
                        search_term=q["search_term"],
                        location=q["location"],
                        results_per_site=25,
                        country_indeed="India",
                        hours_old=24,
                    )
                    new_jobs = [j for j in jobs if j["titleHash"] not in seen_hashes]
                    for j in new_jobs:
                        seen_hashes.add(j["titleHash"])
                    jobspy_jobs.extend(new_jobs)
                    logger.info("  [jobspy] '%s' → %d new jobs", q["search_term"], len(new_jobs))
                except Exception as exc:
                    msg = f"jobspy/{q['search_term']}: {exc}"
                    errors.append(msg)
                    logger.error("  %s", msg)

            jobspy_inserted = _ingest(jobspy_jobs)
            logger.info("[scheduler] JobSpy done — %d inserted", jobspy_inserted)

        except ImportError as exc:
            logger.warning("[scheduler] JobSpy unavailable: %s", exc)
        except Exception as exc:
            msg = f"jobspy/fatal: {exc}"
            errors.append(msg)
            logger.error("[scheduler] JobSpy fatal: %s", exc)

    total = jsearch_inserted + jobspy_inserted
    _status["lastRun"]         = datetime.datetime.utcnow().isoformat()
    _status["lastCount"]       = total
    _status["jsearchInserted"] = jsearch_inserted
    _status["jobspyInserted"]  = jobspy_inserted
    _status["totalQueries"]    = jsearch_queries_done
    _status["errors"]          = errors[-10:]
    logger.info("[scheduler] Done — %d total inserted (jsearch=%d, jobspy=%d)",
                total, jsearch_inserted, jobspy_inserted)


def get_status() -> dict:
    return _status


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_job,
        trigger=IntervalTrigger(minutes=INTERVAL_MINUTES),
        id="scrape_job",
        name="India job scraper (JSearch + JobSpy)",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    logger.info("Scheduler started — every %d min, %d jsearch + %d jobspy queries",
                INTERVAL_MINUTES, len(JSEARCH_QUERIES), len(JOBSPY_QUERIES) if ENABLE_JOBSPY else 0)
    return scheduler
