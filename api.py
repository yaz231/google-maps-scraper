import asyncio
import csv
import io
import json
import logging
import os
import subprocess
import traceback
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import sys

class Logger:
    def info(self, msg, *args):
        print(f"[INFO] {msg % args if args else msg}", file=sys.stderr, flush=True)

    def error(self, msg, *args):
        print(f"[ERROR] {msg % args if args else msg}", file=sys.stderr, flush=True)

    def debug(self, msg, *args):
        print(f"[DEBUG] {msg % args if args else msg}", file=sys.stderr, flush=True)

logger = Logger()

SCRAPER_BINARY = os.getenv("SCRAPER_BINARY", "google-maps-scraper")
SCRAPER_ADDR = os.getenv("SCRAPER_ADDR", "http://localhost:8080")
SCRAPER_DATA_FOLDER = os.getenv("SCRAPER_DATA_FOLDER", "/tmp/gmapsdata")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT", "600"))

scraper_process: subprocess.Popen | None = None


async def wait_for_scraper(timeout: float = 30.0):
    """Wait for the scraper's HTTP server to become reachable."""
    deadline = asyncio.get_event_loop().time() + timeout
    backoff = 0.5
    async with httpx.AsyncClient() as client:
        while asyncio.get_event_loop().time() < deadline:
            try:
                resp = await client.get(f"{SCRAPER_ADDR}/api/v1/jobs", timeout=2.0)
                if resp.status_code < 500:
                    logger.info("Scraper is ready")
                    return
            except (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException):
                pass
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 3.0)
    raise RuntimeError("Scraper did not become ready in time")

async def log_scraper_output(proc: subprocess.Popen):
    """Log scraper's stdout for debugging"""
    for _ in range(20):  # Log first 20 lines
        if proc.stdout:
            line = proc.stdout.readline()
            if line:
                logger.info(f"[SCRAPER] {line.decode().strip()}")
        await asyncio.sleep(0.5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scraper_process
    os.makedirs(SCRAPER_DATA_FOLDER, exist_ok=True)
    cmd = [
        SCRAPER_BINARY,
        "-web",
        "-data-folder", SCRAPER_DATA_FOLDER,
        "-addr", ":8080",
    ]
    logger.info("Starting scraper: %s", " ".join(cmd))
    scraper_process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Merge stderr into stdout
    )
    # Log the first few lines to see if it's starting
    asyncio.create_task(log_scraper_output(scraper_process))
    try:
        await wait_for_scraper()
    except RuntimeError:
        scraper_process.kill()
        raise
    yield
    if scraper_process and scraper_process.poll() is None:
        scraper_process.terminate()
        try:
            scraper_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            scraper_process.kill()


app = FastAPI(title="Google Maps Scraper API", lifespan=lifespan)

# CSV columns produced by the scraper, in order
CSV_HEADERS = [
    "input_id", "link", "title", "category", "address", "open_hours",
    "popular_times", "website", "phone", "plus_code", "review_count",
    "review_rating", "reviews_per_rating", "latitude", "longitude", "cid",
    "status", "descriptions", "reviews_link", "thumbnail", "timezone",
    "price_range", "data_id", "images", "reservations", "order_online",
    "menu", "owner", "complete_address", "about", "user_reviews",
    "user_reviews_extended", "emails",
]

# Fields we want to expose, mapped from CSV column -> output key
FIELD_MAP = {
    "title": "title",
    "cid": "cid",
    "address": "address",
    "latitude": "latitude",
    "longitude": "longitude",
    "review_rating": "rating",
    "review_count": "reviewsCount",
    "phone": "phone",
    "website": "website",
    "open_hours": "openingHours",
    "thumbnail": "imageUrl",
    "images": "imageUrls",
    "category": "category",
}

# Fields that should be parsed as JSON
JSON_FIELDS = {"open_hours", "images"}

# Fields that should be parsed as numbers
FLOAT_FIELDS = {"latitude", "longitude", "review_rating"}
INT_FIELDS = {"review_count"}


def parse_csv_to_shops(csv_text: str) -> list[dict[str, Any]]:
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return []

    headers = rows[0]
    shops = []
    for row in rows[1:]:
        if len(row) != len(headers):
            continue
        raw = dict(zip(headers, row))
        shop: dict[str, Any] = {}
        for csv_col, out_key in FIELD_MAP.items():
            val = raw.get(csv_col, "")
            if csv_col in JSON_FIELDS:
                try:
                    val = json.loads(val) if val else None
                except json.JSONDecodeError:
                    val = None
            elif csv_col in FLOAT_FIELDS:
                try:
                    val = float(val) if val else None
                except (ValueError, TypeError):
                    val = None
            elif csv_col in INT_FIELDS:
                try:
                    val = int(val) if val else None
                except (ValueError, TypeError):
                    val = None
            shop[out_key] = val
        shops.append(shop)
    return shops


class ScrapeRequest(BaseModel):
    city: str = Field(..., min_length=1, description="City name, e.g. 'Austin, TX'")
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    query: str = Field(..., min_length=1, description="Search query, e.g. 'coffee shop'")


class ScrapeResponse(BaseModel):
    success: bool
    count: int
    shops: list[dict[str, Any]]


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape(req: ScrapeRequest):
    logger.info("=== /scrape request received ===")
    logger.info("  city=%s, lat=%s, lng=%s, query=%s", req.city, req.latitude, req.longitude, req.query)

    search_query = f"{req.query} near {req.city}"

    job_payload = {
        "name": search_query,
        "keywords": [search_query],
        "lang": "en",
        "fast_mode": True,
        "zoom": 15,
        "radius": 5000,
        "depth": 1,
        "lat": str(req.latitude),
        "lon": str(req.longitude),
        "max_time": 300,
    }

    logger.info("Job payload: %s", json.dumps(job_payload))

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Create the job
        logger.info("Creating job at %s/api/v1/jobs ...", SCRAPER_ADDR)
        try:
            resp = await client.post(
                f"{SCRAPER_ADDR}/api/v1/jobs",
                json=job_payload,
            )
        except httpx.ConnectError as e:
            logger.error("ConnectError creating job: %s", e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise HTTPException(status_code=503, detail="Scraper service unavailable")
        except Exception as e:
            logger.error("Unexpected error creating job: %s", e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

        logger.info("Job creation response: status=%s, body=%s", resp.status_code, resp.text)

        if resp.status_code != 201:
            logger.error("Scraper rejected job: status=%s, body=%s", resp.status_code, resp.text)
            raise HTTPException(
                status_code=502,
                detail=f"Scraper rejected job: {resp.text}",
            )

        job_id = resp.json()["id"]
        logger.info("Job created with ID: %s", job_id)

        # Poll until finished
        elapsed = 0
        poll_count = 0
        while elapsed < POLL_TIMEOUT:
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            poll_count += 1

            logger.debug("Polling job %s (attempt %d, elapsed %ds/%ds) ...", job_id, poll_count, elapsed, POLL_TIMEOUT)

            try:
                status_resp = await client.get(
                    f"{SCRAPER_ADDR}/api/v1/jobs/{job_id}",
                )
            except httpx.ConnectError as e:
                logger.error("ConnectError polling job %s: %s", job_id, e)
                logger.error("Traceback: %s", traceback.format_exc())
                raise HTTPException(
                    status_code=503,
                    detail="Scraper service unavailable during polling",
                )
            except Exception as e:
                logger.error("Unexpected error polling job %s: %s", job_id, e)
                logger.error("Traceback: %s", traceback.format_exc())
                raise HTTPException(status_code=500, detail=f"Unexpected error polling: {e}")

            if status_resp.status_code != 200:
                logger.info("Polling job %s: non-200 status=%s, body=%s", job_id, status_resp.status_code, status_resp.text)
                continue

            job_data = status_resp.json()
            status = job_data.get("Status", job_data.get("status", ""))
            logger.info("Polling job %s: status=%s", job_id, status)
            logger.debug("Polling job %s: full data=%s", job_id, job_data)

            if status == "ok":
                logger.info("Job %s completed successfully", job_id)
                break
            elif status == "failed":
                logger.error("Job %s failed. Full data: %s", job_id, job_data)
                raise HTTPException(
                    status_code=500,
                    detail="Scraper job failed",
                )
        else:
            logger.error("Job %s timed out after %ds (%d polls)", job_id, POLL_TIMEOUT, poll_count)
            raise HTTPException(
                status_code=504,
                detail=f"Scraper job timed out after {POLL_TIMEOUT}s",
            )

        # Download CSV results
        logger.info("Downloading CSV for job %s ...", job_id)
        try:
            dl_resp = await client.get(
                f"{SCRAPER_ADDR}/api/v1/jobs/{job_id}/download",
            )
        except httpx.ConnectError as e:
            logger.error("ConnectError downloading job %s: %s", job_id, e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise HTTPException(
                status_code=503,
                detail="Scraper service unavailable during download",
            )
        except Exception as e:
            logger.error("Unexpected error downloading job %s: %s", job_id, e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Unexpected error downloading: {e}")

        logger.info("Download response: status=%s, bytes=%d", dl_resp.status_code, len(dl_resp.text))

        if dl_resp.status_code != 200:
            logger.error("Failed to download results for job %s: status=%s, body=%s", job_id, dl_resp.status_code, dl_resp.text)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to download results: {dl_resp.text}",
            )

        logger.debug("CSV preview (first 500 chars): %s", dl_resp.text[:500])

        shops = parse_csv_to_shops(dl_resp.text)
        logger.info("Parsed %d shops from CSV", len(shops))

        # Cap at 50
        shops = shops[:50]

        logger.info("=== /scrape returning %d shops ===", len(shops))
        return ScrapeResponse(success=True, count=len(shops), shops=shops)
