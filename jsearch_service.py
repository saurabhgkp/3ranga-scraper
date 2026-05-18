"""
JSearch (RapidAPI) client — fetches India tech jobs with full descriptions.
"""

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from deduplicator import job_hash

logger = logging.getLogger(__name__)

RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = "jsearch.p.rapidapi.com"
BASE_URL      = f"https://{RAPIDAPI_HOST}/search-v2"

SKILL_PATTERNS = re.compile(
    r"\b("
    r"python|javascript|typescript|java|golang|go|rust|ruby|php|scala|kotlin|swift|c\+\+|c#|r\b"
    r"|react|next\.?js|vue|angular|svelte|nuxt"
    r"|node\.?js|express|fastapi|django|flask|spring|rails|laravel"
    r"|mongodb|postgres|postgresql|mysql|redis|elasticsearch|cassandra|dynamodb|sqlite"
    r"|aws|gcp|azure|docker|kubernetes|terraform|ansible|jenkins|github.actions|ci/cd"
    r"|graphql|rest|grpc|kafka|rabbitmq|celery"
    r"|machine.learning|deep.learning|llm|nlp|pytorch|tensorflow|scikit.learn"
    r"|html|css|tailwind|bootstrap|sass|webpack|vite"
    r"|git|linux|bash|sql|nosql|microservices|api"
    r")\b",
    re.IGNORECASE,
)

JOB_TYPE_MAP = {
    "FULLTIME":   "full-time",
    "PARTTIME":   "part-time",
    "CONTRACTOR": "contract",
    "INTERN":     "internship",
}

SOURCE_MAP = {
    "linkedin":  "linkedin",
    "indeed":    "indeed",
    "glassdoor": "glassdoor",
}


def extract_skills(text: str) -> list[str]:
    if not text:
        return []
    return sorted(set(m.lower() for m in SKILL_PATTERNS.findall(text)))


def _normalise(job: dict[str, Any]) -> dict[str, Any] | None:
    title   = (job.get("job_title") or "").strip()
    company = (job.get("employer_name") or "").strip()
    if not title or not company:
        return None

    location = (job.get("job_location") or
                f"{job.get('job_city','')}, {job.get('job_state','')}".strip(", ") or
                "India")

    description = (job.get("job_description") or "").strip()

    # Date
    dt_str = job.get("job_posted_at_datetime_utc")
    ts     = job.get("job_posted_at_timestamp")
    if dt_str:
        try:
            date_posted = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except ValueError:
            date_posted = datetime.now(timezone.utc)
    elif ts:
        date_posted = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    else:
        date_posted = datetime.now(timezone.utc)

    # Job type
    types = job.get("job_employment_types") or []
    job_type = JOB_TYPE_MAP.get(types[0], "full-time") if types else "full-time"

    # Source
    publisher = (job.get("job_publisher") or "").lower()
    source = next((v for k, v in SOURCE_MAP.items() if k in publisher), "other")

    # Salary
    salary = {
        "min":      job.get("job_min_salary"),
        "max":      job.get("job_max_salary"),
        "currency": "INR",
        "interval": (job.get("job_salary_period") or "yearly").lower(),
    }

    return {
        "title":       title,
        "company":     company,
        "location":    location,
        "isRemote":    bool(job.get("job_is_remote")),
        "description": description,
        "skills":      extract_skills(description),
        "jobType":     job_type,
        "source":      source,
        "applyUrl":    job.get("job_apply_link") or "",
        "datePosted":  date_posted.isoformat(),
        "salary":      salary,
        "titleHash":   job_hash(title, company, location),
    }


class JSearchService:
    def __init__(self):
        if not RAPIDAPI_KEY:
            raise RuntimeError("RAPIDAPI_KEY env var is not set")
        self.headers = {
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key":  RAPIDAPI_KEY,
            "Content-Type":    "application/json",
        }

    def search(self, query: str, num_pages: int = 1) -> list[dict[str, Any]]:
        """Call JSearch and return normalised job list."""
        try:
            resp = httpx.get(
                BASE_URL,
                params={
                    "query":       query,
                    "num_pages":   num_pages,
                    "country":     "in",
                    "date_posted": "today",
                },
                headers=self.headers,
                timeout=30,
            )
            resp.raise_for_status()
            raw_jobs = resp.json().get("data", [])
        except Exception as exc:
            logger.error("JSearch request failed for '%s': %s", query, exc)
            return []

        results = []
        seen: set[str] = set()
        for raw in raw_jobs:
            job = _normalise(raw)
            if job and job["titleHash"] not in seen:
                seen.add(job["titleHash"])
                results.append(job)

        logger.info("  '%s' → %d jobs", query, len(results))
        return results
