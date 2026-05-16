"""
JobSpy Scraper Microservice
FastAPI app — exposes HTTP endpoints for the backend to trigger or monitor scrapes.
"""

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

from scheduler import start_scheduler, run_scrape_job, get_status
from scraper_service import ScraperService

_scheduler = None
_scraper = ScraperService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _scheduler = start_scheduler()
    logger.info("Scraper service started")
    yield
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    logger.info("Scraper service stopped")


app = FastAPI(title="JobAI Scraper", version="1.0.0", lifespan=lifespan)


class ScrapeRequest(BaseModel):
    search_term: str = "software engineer"
    location: str = "United States"
    results_per_site: int = 25


@app.get("/health")
def health():
    return {"status": "ok", "scheduler": _scheduler.running if _scheduler else False}


@app.get("/status")
def status():
    return get_status()


@app.post("/scrape/trigger")
async def trigger_scrape(background_tasks: BackgroundTasks):
    """Manually trigger a full scrape cycle in the background."""
    background_tasks.add_task(run_scrape_job)
    return {"message": "Scrape job triggered"}


@app.post("/scrape/search")
def search_scrape(req: ScrapeRequest):
    """Scrape a specific search term synchronously (for testing)."""
    jobs = _scraper.scrape(
        search_term=req.search_term,
        location=req.location,
        results_per_site=req.results_per_site,
    )
    return {"count": len(jobs), "jobs": jobs[:10]}  # preview first 10


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
