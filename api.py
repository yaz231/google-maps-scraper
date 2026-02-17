import json
import os
import subprocess
import tempfile
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Google Maps Scraper API")

SCRAPER_BINARY = os.getenv("SCRAPER_BINARY", "google-maps-scraper")
SCRAPER_TIMEOUT = int(os.getenv("SCRAPER_TIMEOUT", "300"))


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
def scrape(req: ScrapeRequest):
    search_query = f"{req.query} near {req.city}"

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as query_file:
        query_file.write(search_query + "\n")
        query_path = query_file.name

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as results_file:
        results_path = results_file.name

    try:
        cmd = [
            SCRAPER_BINARY,
            "-input", query_path,
            "-results", results_path,
            "-json",
            "-fast-mode",
            "-geo", f"{req.latitude},{req.longitude}",
            "-radius", "5000",
            "-zoom", "15",
            "-depth", "1",
            "-c", "1",
            "-exit-on-inactivity", "3m",
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SCRAPER_TIMEOUT,
        )

        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise HTTPException(
                status_code=500,
                detail=f"Scraper failed (exit {result.returncode}): {stderr}",
            )

        try:
            with open(results_path, "r") as f:
                content = f.read().strip()

            if not content:
                shops = []
            else:
                shops = json.loads(content)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to parse scraper output: {e}",
            )

        if not isinstance(shops, list):
            shops = [shops]

        # Cap at 50 results
        shops = shops[:50]

        return ScrapeResponse(success=True, count=len(shops), shops=shops)

    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=504,
            detail=f"Scraper timed out after {SCRAPER_TIMEOUT}s",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {e}",
        )
    finally:
        for path in (query_path, results_path):
            try:
                os.unlink(path)
            except OSError:
                pass
