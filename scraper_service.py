"""
JobSpy wrapper — scrapes LinkedIn, Indeed, Glassdoor and normalises results.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from jobspy import scrape_jobs

from deduplicator import job_hash

logger = logging.getLogger(__name__)

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

SOURCES = ["indeed", "glassdoor", "linkedin"]


class ScraperService:
    def extract_skills(self, text: str) -> list[str]:
        if not text:
            return []
        return sorted(set(m.lower() for m in SKILL_PATTERNS.findall(text)))

    def _parse_salary(self, row: pd.Series) -> dict[str, Any]:
        result = {"min": None, "max": None, "currency": "INR", "interval": "yearly"}
        for col in ("min_amount", "max_amount"):
            val = row.get(col)
            if pd.notna(val) and val:
                try:
                    num = float(str(val).replace(",", "").replace("$", "").replace("₹", ""))
                    if col == "min_amount":
                        result["min"] = num
                    elif col == "max_amount":
                        result["max"] = num
                except ValueError:
                    pass
        interval = row.get("interval")
        if pd.notna(interval) and interval:
            result["interval"] = str(interval).lower()
        return result

    def _normalise_row(self, row: pd.Series, source: str) -> dict[str, Any] | None:
        title   = str(row.get("title",   "") or "").strip()
        company = str(row.get("company", "") or "").strip()
        if not title or not company:
            return None

        location    = str(row.get("location", "") or "India").strip()
        description = str(row.get("description", "") or "")

        date_posted = row.get("date_posted")
        if pd.notna(date_posted) and date_posted:
            if isinstance(date_posted, str):
                try:
                    date_posted = datetime.fromisoformat(date_posted).replace(tzinfo=timezone.utc)
                except ValueError:
                    date_posted = datetime.now(timezone.utc)
            elif hasattr(date_posted, "to_pydatetime"):
                date_posted = date_posted.to_pydatetime().replace(tzinfo=timezone.utc)
        else:
            date_posted = datetime.now(timezone.utc)

        job_type_raw = str(row.get("job_type", "") or "").lower()
        job_type_map = {"fulltime": "full-time", "parttime": "part-time",
                        "contract": "contract", "internship": "internship"}
        job_type = job_type_map.get(job_type_raw.replace("-", "").replace(" ", ""), "full-time")

        return {
            "title":       title,
            "company":     company,
            "location":    location,
            "description": description,
            "salary":      self._parse_salary(row),
            "skills":      self.extract_skills(description),
            "source":      source,
            "applyUrl":    str(row.get("job_url", "") or ""),
            "datePosted":  date_posted.isoformat(),
            "isRemote":    bool(row.get("is_remote", False)),
            "jobType":     job_type,
            "titleHash":   job_hash(title, company, location),
        }

    def scrape(
        self,
        search_term: str = "software engineer",
        location: str = "India",
        results_per_site: int = 25,
        country_indeed: str = "India",
        hours_old: int = 24,
    ) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        for source in SOURCES:
            try:
                logger.info("  [jobspy] %s — '%s' in %s", source, search_term, location)
                extra: dict = {}
                if source == "indeed":
                    extra["country_indeed"] = country_indeed
                df = scrape_jobs(
                    site_name=[source],
                    search_term=search_term,
                    location=location,
                    results_wanted=results_per_site,
                    hours_old=hours_old,
                    **extra,
                )
                if df is None or df.empty:
                    logger.info("    no results from %s", source)
                    continue

                for _, row in df.iterrows():
                    try:
                        job = self._normalise_row(row, source)
                        if job and job["titleHash"] not in seen_hashes:
                            seen_hashes.add(job["titleHash"])
                            all_jobs.append(job)
                    except Exception as exc:
                        logger.debug("Row normalise error: %s", exc)

                logger.info("    %d jobs from %s", sum(1 for j in all_jobs if j["source"] == source), source)

            except Exception as exc:
                logger.error("  [jobspy] %s error: %s", source, exc)

        return all_jobs
