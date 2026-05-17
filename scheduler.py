"""
APScheduler — triggers scrape every SCRAPE_INTERVAL_MINUTES and POSTs
results to the backend ingest endpoint.

Sources:
  1. JSearch (RapidAPI) — full descriptions, structured data
  2. JobSpy (web scraping) — Indeed + Glassdoor + LinkedIn, 30+ query terms
Both sources deduplicate by titleHash; no duplicate is ever sent to the DB.
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
NUM_PAGES        = int(os.getenv("JSEARCH_PAGES_PER_QUERY",  "10"))  # 10 jobs/page, max=10
ENABLE_JOBSPY    = os.getenv("ENABLE_JOBSPY", "true").lower() == "true"

# ── JSearch queries (RapidAPI — full descriptions) ─────────────────────────────
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

# ── JobSpy queries (web scraping — Indeed + Glassdoor + LinkedIn) ──────────────
JOBSPY_QUERIES = [
    # ── Core engineering (common title variants) ──────────────────────────
    {"search_term": "Software Engineer"},
    {"search_term": "Software Developer"},          # different listing pool than "engineer"
    {"search_term": "SDE"},                         # Amazon / Flipkart style
    {"search_term": "Full Stack Developer"},
    {"search_term": "Backend Developer"},
    {"search_term": "Backend Engineer"},
    {"search_term": "Frontend Developer"},
    {"search_term": "Frontend Engineer"},
    # ── Languages / frameworks ────────────────────────────────────────────
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
    # ── Mobile ───────────────────────────────────────────────────────────
    {"search_term": "Android Developer"},
    {"search_term": "iOS Developer"},
    {"search_term": "Flutter Developer"},
    {"search_term": "React Native Developer"},
    # ── Infra / Cloud / DevOps ────────────────────────────────────────────
    {"search_term": "DevOps Engineer"},
    {"search_term": "Platform Engineer"},
    {"search_term": "Site Reliability Engineer"},
    {"search_term": "Cloud Engineer"},
    {"search_term": "AWS Solutions Architect"},
    {"search_term": "Kubernetes Engineer"},
    # ── Data / AI / ML ────────────────────────────────────────────────────
    {"search_term": "Data Scientist"},
    {"search_term": "Data Engineer"},
    {"search_term": "Data Analyst"},
    {"search_term": "Machine Learning Engineer"},
    {"search_term": "AI Engineer"},
    {"search_term": "LLM Engineer"},
    {"search_term": "Business Intelligence Analyst"},
    # ── QA ───────────────────────────────────────────────────────────────
    {"search_term": "QA Engineer"},
    {"search_term": "SDET"},
    {"search_term": "Automation Test Engineer"},
    {"search_term": "Manual Tester"},
    # ── Design ───────────────────────────────────────────────────────────
    {"search_term": "UI UX Designer"},
    {"search_term": "Product Designer"},
    # ── Product / Management ──────────────────────────────────────────────
    {"search_term": "Product Manager"},
    {"search_term": "Engineering Manager"},
    {"search_term": "Tech Lead"},
    {"search_term": "Scrum Master"},
    {"search_term": "Business Analyst"},
    {"search_term": "Project Manager IT"},
    # ── Senior / Lead roles ───────────────────────────────────────────────
    {"search_term": "Senior Software Engineer"},
    {"search_term": "Senior Developer"},
    {"search_term": "Principal Engineer"},
    {"search_term": "Staff Engineer"},
    {"search_term": "Solutions Architect"},
    # ── Non-tech ─────────────────────────────────────────────────────────
    {"search_term": "HR Recruiter"},
    {"search_term": "Talent Acquisition Specialist"},
    {"search_term": "Sales Representative"},
    {"search_term": "Business Development Manager"},
    {"search_term": "Account Manager"},
    {"search_term": "Digital Marketing Manager"},
    {"search_term": "SEO Specialist"},
    {"search_term": "Content Writer"},
    # ── Entry level / fresher ─────────────────────────────────────────────
    {"search_term": "Junior Software Developer"},
    {"search_term": "Software Engineer Fresher"},
    {"search_term": "Associate Software Engineer"},
    {"search_term": "Graduate Software Engineer"},
    {"search_term": "Software Engineering Intern"},
    # ── Security ─────────────────────────────────────────────────────────
    {"search_term": "Security Engineer"},
    {"search_term": "Cybersecurity Analyst"},
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
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("inserted", 0)


def run_scrape_job() -> None:
    global _jsearch_service
    logger.info("[scheduler] Scrape started — JSearch (%d queries) + JobSpy (%s, %d queries)",
                len(JSEARCH_QUERIES), "on" if ENABLE_JOBSPY else "off", len(JOBSPY_QUERIES))

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

    for query in (JSEARCH_QUERIES if _jsearch_service else []):
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
            jobspy_jobs = scraper.scrape_all(
                queries=JOBSPY_QUERIES,
                location="India",
                country_indeed="India",
                batch_size=5,
            )
            # filter out hashes already covered by JSearch
            new_jobspy = [j for j in jobspy_jobs if j["titleHash"] not in seen_hashes]
            for j in new_jobspy:
                seen_hashes.add(j["titleHash"])

            jobspy_inserted = _ingest(new_jobspy)
            logger.info("[scheduler] JobSpy done — %d inserted (%d dupes skipped)",
                        jobspy_inserted, len(jobspy_jobs) - len(new_jobspy))

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
    logger.info("Scheduler started — every %d min | jsearch=%d queries | jobspy=%d queries",
                INTERVAL_MINUTES, len(JSEARCH_QUERIES), len(JOBSPY_QUERIES))
    return scheduler
