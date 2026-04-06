"""PSA Population Scraper - FastAPI service.

Scrapes PSA pop report pages for TCG card grade distributions,
matches cards to our catalog, and writes population data to Supabase.

Deployed on Render.com free tier. Triggered by Oracle n8n cron or manual HTTP call.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

from app.config import SCRAPER_API_KEY
from app.scraper import scrape_sets
from app.matcher import match_cards, build_lookup
from app.supabase_client import (
    get_set_mappings,
    get_cards_for_group,
    update_pop_data,
    log_scrape,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Track in-progress scrape so we don't double-trigger
_scrape_lock = asyncio.Lock()
_last_result: dict | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("PSA Pop Scraper starting")
    yield
    logger.info("PSA Pop Scraper shutting down")


app = FastAPI(title="PSA Pop Scraper", lifespan=lifespan)


def _check_auth(request: Request):
    auth = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "").strip()
    if token != SCRAPER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "last_result": _last_result,
        "api_key_len": len(SCRAPER_API_KEY),
        "api_key_prefix": SCRAPER_API_KEY[:4] if len(SCRAPER_API_KEY) > 4 else "short",
    }


@app.get("/status")
async def status():
    return {"last_result": _last_result, "lock_held": _scrape_lock.locked()}


@app.post("/scrape/{game_id}")
async def scrape_game(game_id: str, request: Request, background_tasks: BackgroundTasks):
    _check_auth(request)
    if _scrape_lock.locked():
        raise HTTPException(status_code=409, detail="Scrape already in progress")
    background_tasks.add_task(_run_scrape, game_id)
    return {"message": f"Scrape started for {game_id}", "status": "queued"}


@app.post("/scrape-all")
async def scrape_all(request: Request, background_tasks: BackgroundTasks):
    _check_auth(request)
    if _scrape_lock.locked():
        raise HTTPException(status_code=409, detail="Scrape already in progress")
    background_tasks.add_task(_run_scrape, None)
    return {"message": "Scrape started for all games", "status": "queued"}


async def _run_scrape(game_id: str | None):
    """Core scrape logic. Runs in background task."""
    global _last_result
    async with _scrape_lock:
        start = time.time()
        total_matched = 0
        total_unmatched = 0
        total_updated = 0
        games_processed = []

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # Get set mappings
                mappings = await get_set_mappings(client, game_id)
                if not mappings:
                    _last_result = {
                        "status": "skipped",
                        "reason": f"No set mappings found for {game_id or 'any game'}",
                        "duration_ms": int((time.time() - start) * 1000),
                    }
                    await log_scrape(client, _last_result)
                    return

                # Group mappings by game_id
                by_game: dict[str, list[dict]] = {}
                for m in mappings:
                    by_game.setdefault(m["game_id"], []).append(m)

                for gid, sets in by_game.items():
                    logger.info(f"Scraping {gid}: {len(sets)} sets")

                    # Scrape all sets for this game
                    scraped_by_set = await scrape_sets(sets)

                    # Match and update per set
                    for mapping in sets:
                        scraped_cards = scraped_by_set.get(mapping["psa_set_id"], [])
                        if not scraped_cards:
                            continue

                        # Get our catalog cards for this group
                        our_cards = await get_cards_for_group(client, mapping["group_id"])
                        if not our_cards:
                            logger.warning(f"No cards in DB for group {mapping['group_id']}")
                            continue

                        lookup = build_lookup(our_cards)
                        matched, unmatched = match_cards(scraped_cards, lookup)
                        total_matched += len(matched)
                        total_unmatched += len(unmatched)

                        if unmatched:
                            logger.info(
                                f"  {mapping['psa_set_slug']}: {len(unmatched)} unmatched: "
                                + ", ".join(u["card_name"][:30] for u in unmatched[:5])
                            )

                        # Write population data to Supabase
                        if matched:
                            updates = [
                                {
                                    "tcg_product_id": c["tcg_product_id"],
                                    "psa10_pop": c["psa10_pop"],
                                    "total_pop": c["total_pop"],
                                }
                                for c in matched
                            ]
                            count = await update_pop_data(client, updates)
                            total_updated += count

                    games_processed.append(gid)

            duration_ms = int((time.time() - start) * 1000)
            _last_result = {
                "status": "success",
                "games": games_processed,
                "sets_scraped": len(mappings),
                "matched": total_matched,
                "unmatched": total_unmatched,
                "updated": total_updated,
                "duration_ms": duration_ms,
            }
            logger.info(f"Scrape complete: {_last_result}")
            await log_scrape(client, _last_result)

        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            _last_result = {
                "status": "failed",
                "error": str(e),
                "duration_ms": duration_ms,
            }
            logger.error(f"Scrape failed: {e}", exc_info=True)
            try:
                async with httpx.AsyncClient(timeout=10) as log_client:
                    await log_scrape(log_client, _last_result)
            except Exception:
                pass
