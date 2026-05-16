"""
JSearch Scraper Microservice
FastAPI app — exposes HTTP endpoints for the backend to trigger or monitor scrapes.
"""

import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

from scheduler import start_scheduler, run_scrape_job, get_status
from jsearch_service import JSearchService

_scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    _scheduler = start_scheduler()
    logger.info("JSearch scraper service started")
    yield
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    logger.info("Scraper service stopped")


app = FastAPI(title="3ranga JSearch Scraper", version="2.0.0", lifespan=lifespan)


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


@app.get("/scrape/test")
def test_scrape():
    """Quick test — fetch 1 query and return raw results (no DB write)."""
    svc = JSearchService()
    jobs = svc.search("software engineer jobs in Bangalore India", num_pages=1)
    return {"count": len(jobs), "sample": jobs[:3]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
