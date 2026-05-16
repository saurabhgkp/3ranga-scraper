"""
JobSpy wrapper — scrapes Indeed, Glassdoor, LinkedIn with retry, UA rotation,
and proxy support. Logic ported from jobspy_script.py.
"""

import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
from jobspy import scrape_jobs

from deduplicator import job_hash

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
RESULTS_WANTED   = int(os.getenv("JOBSPY_RESULTS_PER_SITE", "50"))
HOURS_OLD        = int(os.getenv("JOBSPY_HOURS_OLD", "24"))
ENABLE_LI_DESC   = os.getenv("JOBSPY_LINKEDIN_DESCRIPTIONS", "false").lower() == "true"
SITES            = ["indeed", "glassdoor", "linkedin"]

# ── User-agent rotation ────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
]

# ── Proxy support ──────────────────────────────────────────────────────────────
def _load_proxies() -> list[str]:
    raw = os.getenv("PROXY_LIST", "").strip()
    if raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    return []

PROXIES = _load_proxies()

# ── Skill extraction ───────────────────────────────────────────────────────────
SKILL_KEYWORDS = [
    "javascript", "typescript", "python", "java", "c++", "c#", "go", "golang",
    "rust", "php", "ruby", "scala", "kotlin", "swift",
    "react", "angular", "vue", "next.js", "nuxt", "svelte",
    "html", "css", "tailwind", "bootstrap", "sass", "webpack", "vite",
    "node.js", "nestjs", "express", "fastapi", "django", "flask",
    "spring", "laravel", "rails",
    "react native", "flutter", "android", "ios",
    "mongodb", "postgresql", "mysql", "redis", "elasticsearch",
    "cassandra", "dynamodb", "sqlite", "firebase", "supabase",
    "docker", "kubernetes", "aws", "azure", "gcp", "terraform",
    "ansible", "jenkins", "ci/cd", "github actions",
    "graphql", "rest", "grpc", "kafka", "rabbitmq", "celery",
    "selenium", "cypress", "playwright", "testng", "junit",
    "machine learning", "deep learning", "llm", "nlp",
    "pytorch", "tensorflow", "scikit-learn", "pandas", "numpy",
    "sql", "nosql", "microservices", "api",
    "power bi", "tableau", "data visualization",
    "agile", "scrum", "jira",
    "figma", "adobe xd", "photoshop",
    "salesforce", "seo", "sem", "google ads",
    "linux", "bash", "git",
]

SKILL_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in SKILL_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

JOB_TYPE_MAP = {
    "fulltime":    "full-time",
    "parttime":    "part-time",
    "contract":    "contract",
    "internship":  "internship",
    "temporary":   "contract",
}

INDIA_TOP_CITIES = [
    "Bangalore", "Mumbai", "Delhi", "Hyderabad", "Chennai", "Pune",
    "Kolkata", "Noida", "Gurgaon", "Ahmedabad", "Jaipur", "Lucknow",
    "Indore", "Thane", "Bhopal", "Visakhapatnam",
]


def extract_skills(text: str) -> list[str]:
    if not text:
        return []
    return sorted(set(m.lower() for m in SKILL_RE.findall(str(text))))


def _proxy_for(term_idx: int, site_idx: int) -> Optional[str]:
    if not PROXIES:
        return None
    return PROXIES[(term_idx + site_idx) % len(PROXIES)]


def _safe_scrape(site: str, search_term: str, term_idx: int, site_idx: int,
                 location: str = "India", country_indeed: str = "India") -> pd.DataFrame:
    """Scrape one site with UA rotation, proxy, and 4-attempt backoff."""
    max_retries = 4
    backoff = [2, 5, 10]

    for attempt in range(max_retries):
        try:
            delay = random.uniform(1.5, 3.0) * (1.5 ** attempt)
            time.sleep(delay)

            proxy = _proxy_for(term_idx, site_idx)
            params: dict[str, Any] = {
                "site_name":      [site],
                "search_term":    search_term,
                "location":       location,
                "results_wanted": RESULTS_WANTED,
                "hours_old":      HOURS_OLD,
                "headers":        {"User-Agent": random.choice(USER_AGENTS)},
                "verbose":        0,
            }
            if site == "indeed":
                params["country_indeed"] = country_indeed
            if site == "linkedin" and ENABLE_LI_DESC:
                params["linkedin_fetch_description"] = True
            if proxy:
                params["proxies"] = proxy

            df = scrape_jobs(**params)

            if df is not None and not df.empty:
                logger.info("    [jobspy/%s] '%s' → %d rows (attempt %d)",
                            site, search_term, len(df), attempt + 1)
                # retry if suspiciously low and we have attempts left
                if len(df) < 5 and attempt < max_retries - 1:
                    logger.warning("    low count, retrying...")
                    continue
                return df

            logger.info("    [jobspy/%s] '%s' → 0 rows", site, search_term)

        except Exception as exc:
            logger.warning("    [jobspy/%s] attempt %d error: %s", site, attempt + 1, exc)
            if attempt < max_retries - 1:
                time.sleep(backoff[attempt])
            else:
                logger.error("    [jobspy/%s] failed after %d attempts", site, max_retries)

    return pd.DataFrame()


def _normalise_row(row: pd.Series, source: str) -> dict[str, Any] | None:
    title   = str(row.get("title",   "") or "").strip()
    company = str(row.get("company", "") or "").strip()
    if not title or not company:
        return None

    location = str(row.get("location", "") or "India").strip()
    if not location or location in ("nan", "None"):
        for city in INDIA_TOP_CITIES:
            if city.lower() in title.lower() + company.lower():
                location = city + ", India"
                break
        else:
            location = "India"

    description = str(row.get("description", "") or "")

    # Date
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

    # Job type
    jt_raw = str(row.get("job_type", "") or "").lower().replace("-", "").replace(" ", "")
    job_type = JOB_TYPE_MAP.get(jt_raw, "full-time")

    # Salary
    salary: dict[str, Any] = {"min": None, "max": None, "currency": "INR", "interval": "yearly"}
    for col, key in (("min_amount", "min"), ("max_amount", "max")):
        val = row.get(col)
        if pd.notna(val) and val:
            try:
                salary[key] = float(str(val).replace(",", "").replace("$", "").replace("₹", ""))
            except ValueError:
                pass
    interval = row.get("interval")
    if pd.notna(interval) and interval:
        salary["interval"] = str(interval).lower()

    return {
        "title":       title,
        "company":     company,
        "location":    location,
        "description": description,
        "skills":      extract_skills(description),
        "jobType":     job_type,
        "source":      source,
        "applyUrl":    str(row.get("job_url", "") or ""),
        "datePosted":  date_posted.isoformat(),
        "isRemote":    bool(row.get("is_remote", False)),
        "salary":      salary,
        "titleHash":   job_hash(title, company, location),
    }


class ScraperService:
    def scrape_all(
        self,
        queries: list[dict],
        location: str = "India",
        country_indeed: str = "India",
        batch_size: int = 5,
        batch_delay: tuple[float, float] = (3.0, 6.0),
    ) -> list[dict[str, Any]]:
        """
        Run all queries across all sites with batching and deduplication.
        queries: list of {"search_term": str} dicts.
        """
        all_jobs: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        batch_count = (len(queries) + batch_size - 1) // batch_size

        for batch_idx in range(0, len(queries), batch_size):
            batch = queries[batch_idx: batch_idx + batch_size]
            logger.info("  [jobspy] batch %d/%d (%d terms)",
                        batch_idx // batch_size + 1, batch_count, len(batch))

            for term_idx, q in enumerate(batch):
                global_idx = batch_idx + term_idx
                search_term = q["search_term"]

                for site_idx, site in enumerate(SITES):
                    df = _safe_scrape(site, search_term, global_idx, site_idx,
                                      location=location, country_indeed=country_indeed)
                    if df.empty:
                        continue

                    for _, row in df.iterrows():
                        try:
                            job = _normalise_row(row, site)
                            if job and job["titleHash"] not in seen_hashes:
                                seen_hashes.add(job["titleHash"])
                                all_jobs.append(job)
                        except Exception as exc:
                            logger.debug("normalise error: %s", exc)

                # short pause between terms
                if term_idx < len(batch) - 1:
                    time.sleep(random.uniform(1.0, 2.0))

            # longer pause between batches
            if batch_idx + batch_size < len(queries):
                pause = random.uniform(*batch_delay)
                logger.info("  [jobspy] batch done — sleeping %.1fs", pause)
                time.sleep(pause)

        logger.info("  [jobspy] total unique jobs: %d", len(all_jobs))
        return all_jobs
