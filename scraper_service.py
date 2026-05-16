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
from proxy_rotator import ProxyRotator

logger = logging.getLogger(__name__)

# Common tech skills for extraction
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

SOURCES = ["linkedin", "indeed", "glassdoor"]


class ScraperService:
    def __init__(self):
        self.proxy_rotator = ProxyRotator()

    def extract_skills(self, text: str) -> list[str]:
        if not text:
            return []
        found = set(m.lower() for m in SKILL_PATTERNS.findall(text))
        return sorted(found)

    def _parse_salary(self, row: pd.Series) -> dict[str, Any]:
        """Extract min/max salary from jobspy row."""
        result = {"min": None, "max": None, "currency": "USD", "interval": "yearly"}
        for col in ("min_amount", "max_amount", "salary"):
            val = row.get(col)
            if pd.notna(val) and val:
                try:
                    num = float(str(val).replace(",", "").replace("$", ""))
                    if col == "min_amount":
                        result["min"] = num
                    elif col == "max_amount":
                        result["max"] = num
                    elif not result["min"]:
                        result["min"] = num
                except ValueError:
                    pass
        interval = row.get("interval")
        if pd.notna(interval) and interval:
            result["interval"] = str(interval)
        currency = row.get("currency")
        if pd.notna(currency) and currency:
            result["currency"] = str(currency)
        return result

    def _normalise_row(self, row: pd.Series, source: str) -> dict[str, Any]:
        """Convert a jobspy DataFrame row into our standard job document."""
        title = str(row.get("title", "")).strip()
        company = str(row.get("company", "")).strip()
        location = str(row.get("location", "") or "Remote").strip()
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

        return {
            "title": title,
            "company": company,
            "location": location,
            "description": description,
            "salary": self._parse_salary(row),
            "skills": self.extract_skills(description + " " + str(row.get("job_type", ""))),
            "source": source,
            "applyUrl": str(row.get("job_url", "") or ""),
            "datePosted": date_posted.isoformat(),
            "isRemote": bool(row.get("is_remote", False)),
            "jobType": str(row.get("job_type", "") or ""),
            "titleHash": job_hash(title, company, location),
        }

    def scrape(
        self,
        search_term: str = "software engineer",
        location: str = "India",
        results_per_site: int = 25,
        country_indeed: str = "India",
    ) -> list[dict[str, Any]]:
        """Scrape all configured sources and return normalised job list."""
        all_jobs: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        proxy = self.proxy_rotator.next()
        proxy_settings = {"proxies": {"http": proxy, "https": proxy}} if proxy else {}

        for source in SOURCES:
            try:
                logger.info("Scraping %s — '%s' in %s", source, search_term, location)
                extra: dict = {}
                if source == "indeed":
                    extra["country_indeed"] = country_indeed
                if source == "linkedin":
                    extra["linkedin_fetch_description"] = True
                df = scrape_jobs(
                    site_name=[source],
                    search_term=search_term,
                    location=location,
                    results_wanted=results_per_site,
                    hours_old=5,
                    **extra,
                    **proxy_settings,
                )
                if df is None or df.empty:
                    logger.info("  No results from %s", source)
                    continue

                for _, row in df.iterrows():
                    try:
                        job = self._normalise_row(row, source)
                        if not job["title"] or not job["company"]:
                            continue
                        h = job["titleHash"]
                        if h not in seen_hashes:
                            seen_hashes.add(h)
                            all_jobs.append(job)
                    except Exception as exc:
                        logger.debug("Row normalise error: %s", exc)

                logger.info("  %d unique jobs from %s", len([j for j in all_jobs if j["source"] == source]), source)

            except Exception as exc:
                logger.error("Scrape error for %s: %s", source, exc)
                if proxy:
                    self.proxy_rotator.mark_bad(proxy)

        logger.info("Total unique jobs scraped: %d", len(all_jobs))
        return all_jobs
