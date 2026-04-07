"""PSA Population + Sales History Scraper - FastAPI service.

Two scraping modes:
1. Pop report (/scrape/{game_id}): bulk population data via PSA's /Pop/GetSetItems JSON API (curl_cffi)
2. Sales history (/scrape-sales/{game_id}): per-card sales via PSA spec pages (Playwright headless Chromium)

Deployed on Render.com free tier (512 MB). Triggered by Oracle n8n cron or manual HTTP call.
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

from app.config import SCRAPER_API_KEY
from app.scraper import scrape_sets, discover_all_sets
from app.sales_scraper import scrape_sales_batch
from app.matcher import match_cards, build_lookup, normalize
from app.supabase_client import (
    get_set_mappings,
    get_cards_for_group,
    get_all_cards_for_game,
    update_pop_data,
    get_spec_ids_for_game,
    upsert_sales_history,
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
    return {"status": "ok", "last_result": _last_result}


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


@app.post("/scrape-sales/{game_id}")
async def scrape_sales(game_id: str, request: Request, background_tasks: BackgroundTasks):
    """Scrape PSA sales history for cards in a game. Uses Playwright (heavier, ~300MB RAM)."""
    _check_auth(request)
    if _scrape_lock.locked():
        raise HTTPException(status_code=409, detail="Scrape already in progress")
    background_tasks.add_task(_run_sales_scrape, game_id)
    return {"message": f"Sales scrape started for {game_id}", "status": "queued"}


async def _run_scrape(game_id: str | None):
    """Core scrape logic. Runs in background task."""
    global _last_result
    async with _scrape_lock:
        start = time.time()
        total_matched = 0
        total_unmatched = 0
        total_updated = 0
        games_processed = []

        client = httpx.AsyncClient(timeout=60)
        try:
            # Auto-discover all sets from PSA website
            logger.info("Discovering sets from PSA...")
            all_discovered = await discover_all_sets()
            if not all_discovered:
                _last_result = {
                    "status": "failed",
                    "reason": "Set discovery returned 0 sets (PSA may be blocking)",
                    "duration_ms": int((time.time() - start) * 1000),
                }
                await log_scrape(client, _last_result)
                return

            # Filter to requested game if specified
            if game_id:
                all_discovered = [s for s in all_discovered if s["game_id"] == game_id]

            logger.info(f"Discovered {len(all_discovered)} sets to scrape")

            # Group by game_id
            by_game: dict[str, list[dict]] = {}
            for m in all_discovered:
                by_game.setdefault(m["game_id"], []).append(m)

            for gid, sets in by_game.items():
                logger.info(f"Scraping {gid}: {len(sets)} sets")

                # Get ALL our catalog cards for this game (once, reuse for all sets)
                our_cards = await get_all_cards_for_game(client, gid)
                if not our_cards:
                    logger.warning(f"No cards in DB for game {gid}")
                    continue
                lookup = build_lookup(our_cards)
                logger.info(f"  Loaded {len(our_cards)} catalog cards for matching")

                # Scrape all sets for this game
                scraped_by_set = await scrape_sets(sets)

                # Match all scraped cards against our catalog
                for s in sets:
                    scraped_cards = scraped_by_set.get(s["psa_set_id"], [])
                    if not scraped_cards:
                        continue

                    matched, unmatched = match_cards(scraped_cards, lookup)
                    total_matched += len(matched)
                    total_unmatched += len(unmatched)

                    # Write population data to Supabase (full grade distribution)
                    if matched:
                        updates = [
                            {
                                "tcg_product_id": c["tcg_product_id"],
                                "psa9_pop": c.get("psa9_pop", 0),
                                "psa10_pop": c.get("psa10_pop", 0),
                                "total_pop": c.get("total_pop", 0),
                                "spec_id": c.get("spec_id", 0),
                                "game_id": gid,
                                "psa_set_id": s["psa_set_id"],
                                "card_name": c.get("card_name", ""),
                                "card_number": c.get("card_number", ""),
                                "variant": c.get("variant", ""),
                                # Full grade distribution
                                "grade_authentic": c.get("grade_authentic", 0),
                                "grade_1": c.get("grade_1", 0),
                                "grade_1_5": c.get("grade_1_5", 0),
                                "grade_2": c.get("grade_2", 0),
                                "grade_2_5": c.get("grade_2_5", 0),
                                "grade_3": c.get("grade_3", 0),
                                "grade_3_5": c.get("grade_3_5", 0),
                                "grade_4": c.get("grade_4", 0),
                                "grade_4_5": c.get("grade_4_5", 0),
                                "grade_5": c.get("grade_5", 0),
                                "grade_5_5": c.get("grade_5_5", 0),
                                "grade_6": c.get("grade_6", 0),
                                "grade_6_5": c.get("grade_6_5", 0),
                                "grade_7": c.get("grade_7", 0),
                                "grade_7_5": c.get("grade_7_5", 0),
                                "grade_8": c.get("grade_8", 0),
                                "grade_8_5": c.get("grade_8_5", 0),
                                "half_grade_total": c.get("half_grade_total", 0),
                                "qualified_total": c.get("qualified_total", 0),
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
                "sets_scraped": len(all_discovered),
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
                await log_scrape(client, _last_result)
            except Exception:
                pass
        finally:
            await client.aclose()


async def _run_sales_scrape(game_id: str):
    """Scrape PSA sales history for cards in a game. Uses Playwright."""
    global _last_result
    async with _scrape_lock:
        start = time.time()
        client = httpx.AsyncClient(timeout=60)
        try:
            # Get spec IDs for this game from our pop data
            mappings = await get_set_mappings(client, game_id)
            if not mappings:
                _last_result = {
                    "status": "skipped",
                    "job": "sales",
                    "reason": f"No set mappings for {game_id}",
                    "duration_ms": int((time.time() - start) * 1000),
                }
                return

            # Collect spec_ids by scraping pop data for each set mapping
            # Also build spec_id -> tcg_product_id lookup for matching sales
            all_spec_ids = set()
            spec_to_tcg: dict[int, int] = {}

            for mapping in mappings:
                our_cards = await get_cards_for_group(client, mapping["group_id"])
                if not our_cards:
                    continue

                lookup = build_lookup(our_cards)
                scraped_by_set = await scrape_sets([mapping])
                from app.scraper import parse_cards
                raw_cards = scraped_by_set.get(mapping["psa_set_id"], [])

                for c in raw_cards:
                    spec_id = c.get("spec_id", 0)
                    if not spec_id or spec_id <= 0:
                        continue
                    all_spec_ids.add(spec_id)

                    # Match this card to our catalog for tcg_product_id
                    norm_name = normalize(c.get("card_name", ""))
                    if norm_name in lookup:
                        spec_to_tcg[spec_id] = lookup[norm_name]

            spec_list = sorted(all_spec_ids)
            logger.info(f"Sales scrape {game_id}: {len(spec_list)} spec IDs, {len(spec_to_tcg)} matched to catalog")

            if not spec_list:
                _last_result = {
                    "status": "skipped",
                    "job": "sales",
                    "reason": f"No spec IDs found for {game_id}",
                    "duration_ms": int((time.time() - start) * 1000),
                }
                return

            # Scrape sales in batches via Playwright
            results = await scrape_sales_batch(spec_list)

            # Flatten, match tcg_product_id, and insert
            all_sales = []
            for spec_id, sales in results.items():
                for s in sales:
                    s["game_id"] = game_id
                    s["tcg_product_id"] = spec_to_tcg.get(spec_id)
                all_sales.extend(sales)

            inserted = await upsert_sales_history(client, all_sales)

            duration_ms = int((time.time() - start) * 1000)
            _last_result = {
                "status": "success",
                "job": "sales",
                "game": game_id,
                "specs_scraped": len(spec_list),
                "sales_found": len(all_sales),
                "sales_inserted": inserted,
                "duration_ms": duration_ms,
            }
            logger.info(f"Sales scrape complete: {_last_result}")
            await log_scrape(client, _last_result)

        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            _last_result = {
                "status": "failed",
                "job": "sales",
                "error": str(e),
                "duration_ms": duration_ms,
            }
            logger.error(f"Sales scrape failed: {e}", exc_info=True)
            try:
                await log_scrape(client, _last_result)
            except Exception:
                pass
        finally:
            await client.aclose()
