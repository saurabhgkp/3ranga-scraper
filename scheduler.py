"""
APScheduler — triggers scrape every SCRAPE_INTERVAL_MINUTES and POSTs
results to the backend ingest endpoint.

Sources:
  1. JSearch (RapidAPI) — full descriptions, structured data
  2. JobSpy (web scraping) — Indeed + Glassdoor + LinkedIn
Both sources deduplicate by titleHash; no duplicate is ever sent to the DB.

Search terms are fetched dynamically from the backend DB before each run so
that the admin panel changes take effect without redeploying the scraper.
Hardcoded lists below serve as a fallback if the fetch fails.
"""

import datetime
import logging
import math
import os
import time

import httpx
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from jsearch_service import JSearchService

logger = logging.getLogger(__name__)

BACKEND_URL      = os.getenv("BACKEND_URL",              "http://localhost:4000")
INGEST_SECRET    = os.getenv("INGEST_SECRET",            "internal-scraper-secret")
INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "240"))
NUM_PAGES        = int(os.getenv("JSEARCH_PAGES_PER_QUERY",  "10"))  # 10 jobs/page, max=10
ENABLE_JOBSPY    = os.getenv("ENABLE_JOBSPY", "true").lower() == "true"
TERMS_PER_RUN    = int(os.getenv("JOBSPY_TERMS_PER_RUN", "35"))  # jobspy terms per rotation slot

# ── Hardcoded fallback terms (used only when backend fetch fails) ──────────────
_FALLBACK_JSEARCH = [
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

_FALLBACK_JOBSPY = [
    {"search_term": "Software Engineer"},
    {"search_term": "Software Developer"},
    {"search_term": "SDE"},
    {"search_term": "Full Stack Developer"},
    {"search_term": "Backend Developer"},
    {"search_term": "Backend Engineer"},
    {"search_term": "Frontend Developer"},
    {"search_term": "Frontend Engineer"},
    {"search_term": "Node.js Developer"},
    {"search_term": "React Developer"},
    {"search_term": "Python Developer"},
    {"search_term": "Python Engineer"},
    {"search_term": "Java Developer"},
    {"search_term": "Java Engineer"},
    {"search_term": "Go Developer"},
    {"search_term": "Golang Developer"},
    {"search_term": "TypeScript Developer"},
    {"search_term": "PHP Developer"},
    {"search_term": "Ruby on Rails Developer"},
    {"search_term": "Spring Boot Developer"},
    {"search_term": "Android Developer"},
    {"search_term": "iOS Developer"},
    {"search_term": "Flutter Developer"},
    {"search_term": "React Native Developer"},
    {"search_term": "DevOps Engineer"},
    {"search_term": "Platform Engineer"},
    {"search_term": "Site Reliability Engineer"},
    {"search_term": "Cloud Engineer"},
    {"search_term": "AWS Solutions Architect"},
    {"search_term": "Kubernetes Engineer"},
    {"search_term": "Data Scientist"},
    {"search_term": "Data Engineer"},
    {"search_term": "Data Analyst"},
    {"search_term": "Machine Learning Engineer"},
    {"search_term": "AI Engineer"},
    {"search_term": "LLM Engineer"},
    {"search_term": "Business Intelligence Analyst"},
    {"search_term": "QA Engineer"},
    {"search_term": "SDET"},
    {"search_term": "Automation Test Engineer"},
    {"search_term": "Manual Tester"},
    {"search_term": "UI UX Designer"},
    {"search_term": "Product Designer"},
    {"search_term": "Product Manager"},
    {"search_term": "Engineering Manager"},
    {"search_term": "Tech Lead"},
    {"search_term": "Scrum Master"},
    {"search_term": "Business Analyst"},
    {"search_term": "Project Manager IT"},
    {"search_term": "Senior Software Engineer"},
    {"search_term": "Senior Developer"},
    {"search_term": "Principal Engineer"},
    {"search_term": "Staff Engineer"},
    {"search_term": "Solutions Architect"},
    {"search_term": "HR Recruiter"},
    {"search_term": "Talent Acquisition Specialist"},
    {"search_term": "Sales Representative"},
    {"search_term": "Business Development Manager"},
    {"search_term": "Account Manager"},
    {"search_term": "Digital Marketing Manager"},
    {"search_term": "SEO Specialist"},
    {"search_term": "Content Writer"},
    {"search_term": "Junior Software Developer"},
    {"search_term": "Software Engineer Fresher"},
    {"search_term": "Associate Software Engineer"},
    {"search_term": "Graduate Software Engineer"},
    {"search_term": "Software Engineering Intern"},
    {"search_term": "Security Engineer"},
    {"search_term": "Cybersecurity Analyst"},
]

_jsearch_service = None  # initialised lazily on first run
_scheduler_ref   = None  # set by start_scheduler so run_scrape_job can reschedule
_current_interval = INTERVAL_MINUTES
_status: dict = {
    "lastRun": None, "lastCount": 0,
    "jsearchInserted": 0, "jobspyInserted": 0,
    "totalQueries": 0, "errors": [],
}


def _fetch_interval() -> int:
    """Fetch the configured scrape interval from backend DB. Falls back to env var."""
    try:
        resp = httpx.get(
            f"{BACKEND_URL}/api/admin/scraper/config",
            headers={"x-scraper-secret": INGEST_SECRET},
            timeout=5,
        )
        resp.raise_for_status()
        val = resp.json().get("intervalMinutes")
        if isinstance(val, int) and 30 <= val <= 10080:
            return val
    except Exception as exc:
        logger.warning("[scheduler] Could not fetch interval from DB (%s) — using %dmin", exc, INTERVAL_MINUTES)
    return INTERVAL_MINUTES


def _fetch_search_terms() -> tuple[list[str], list[dict]]:
    """Fetch enabled search terms from DB via backend API.
    Returns (jsearch_queries, jobspy_queries).
    Falls back to hardcoded lists on any error.
    """
    try:
        resp = httpx.get(
            f"{BACKEND_URL}/api/admin/scraper/search-terms",
            headers={"x-scraper-secret": INGEST_SECRET},
            timeout=10,
        )
        resp.raise_for_status()
        terms = resp.json().get("terms", [])
        if not terms:
            raise ValueError("Empty terms list from backend")

        jsearch = [t["term"] for t in terms if t["type"] in ("jsearch", "both")]
        jobspy  = [{"search_term": t["term"]} for t in terms if t["type"] in ("jobspy", "both")]

        logger.info("[scheduler] Loaded %d jsearch + %d jobspy terms from DB",
                    len(jsearch), len(jobspy))
        return jsearch, jobspy
    except Exception as exc:
        logger.warning("[scheduler] Failed to fetch terms from DB (%s) — using fallback", exc)
        return _FALLBACK_JSEARCH, _FALLBACK_JOBSPY


def _ingest(jobs: list) -> int:
    if not jobs:
        return 0
    resp = httpx.post(
        f"{BACKEND_URL}/api/jobs/ingest",
        json={"jobs": jobs},
        headers={"x-scraper-secret": INGEST_SECRET},
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("inserted", 0)


def run_scrape_job() -> None:
    global _jsearch_service, _scheduler_ref, _current_interval

    # ── Check if interval changed; reschedule if so ───────────────────────────
    new_interval = _fetch_interval()
    if new_interval != _current_interval and _scheduler_ref is not None:
        try:
            _scheduler_ref.reschedule_job(
                "scrape_job",
                trigger=IntervalTrigger(minutes=new_interval),
            )
            logger.info("[scheduler] Interval updated %dmin → %dmin", _current_interval, new_interval)
            _current_interval = new_interval
        except Exception as exc:
            logger.warning("[scheduler] Could not reschedule: %s", exc)

    # ── Load search terms from DB (falls back to hardcoded on error) ──────────
    jsearch_queries, jobspy_queries = _fetch_search_terms()

    # ── Rotate jobspy terms: pick a time-based slice so each run covers a
    #    different subset; full list cycles every ~24 h at 4-h intervals ──────
    if jobspy_queries and TERMS_PER_RUN < len(jobspy_queries):
        num_slots  = math.ceil(len(jobspy_queries) / TERMS_PER_RUN)
        slot       = int(time.time() / (INTERVAL_MINUTES * 60)) % num_slots
        start      = slot * TERMS_PER_RUN
        jobspy_queries = (jobspy_queries + jobspy_queries)[start: start + TERMS_PER_RUN]
        logger.info("[scheduler] JobSpy rotation: slot %d/%d → %d terms",
                    slot + 1, num_slots, len(jobspy_queries))

    logger.info("[scheduler] Scrape started — JSearch (%d queries) + JobSpy (%s, %d queries)",
                len(jsearch_queries), "on" if ENABLE_JOBSPY else "off", len(jobspy_queries))

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

    for query in (jsearch_queries if _jsearch_service else []):
        try:
            jobs = _jsearch_service.search(query, num_pages=NUM_PAGES)
            new_jobs = [j for j in jobs if j["titleHash"] not in seen_hashes]
            for j in new_jobs:
                seen_hashes.add(j["titleHash"])
            jsearch_jobs.extend(new_jobs)
            jsearch_queries_done += 1
            logger.info("  [jsearch] '%s' → %d new", query[:50], len(new_jobs))
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

            def _on_batch(batch_jobs: list) -> None:
                """Ingest each batch immediately so DB updates progressively."""
                global jobspy_inserted
                new = [j for j in batch_jobs if j["titleHash"] not in seen_hashes]
                for j in new:
                    seen_hashes.add(j["titleHash"])
                if new:
                    inserted = _ingest(new)
                    jobspy_inserted += inserted
                    logger.info("[scheduler] Batch ingested — %d new jobs (total so far: %d)",
                                inserted, jobspy_inserted)

            scraper.scrape_all(
                queries=jobspy_queries,
                location="India",
                country_indeed="India",
                batch_size=5,
                on_batch=_on_batch,
            )
            logger.info("[scheduler] JobSpy done — %d total inserted", jobspy_inserted)

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
    logger.info("[scheduler] Done — %d total (jsearch=%d jobspy=%d)",
                total, jsearch_inserted, jobspy_inserted)


def get_status() -> dict:
    return _status


def start_scheduler() -> BackgroundScheduler:
    global _scheduler_ref, _current_interval
    initial_interval = _fetch_interval()
    _current_interval = initial_interval

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scrape_job,
        trigger=IntervalTrigger(minutes=initial_interval),
        id="scrape_job",
        name="India job scraper (JSearch + JobSpy)",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    _scheduler_ref = scheduler
    logger.info("Scheduler started — every %d min | terms fetched dynamically from DB",
                initial_interval)
    return scheduler
