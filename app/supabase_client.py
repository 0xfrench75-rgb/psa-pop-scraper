"""Supabase REST API wrapper using httpx (lighter than supabase-py)."""

import logging
import httpx
from datetime import datetime, timezone
from app.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

REST_URL = f"{SUPABASE_URL}/rest/v1"


async def get_set_mappings(client: httpx.AsyncClient, game_id: str | None = None) -> list[dict]:
    """Fetch PSA set mappings. If game_id is None, fetch all."""
    url = f"{REST_URL}/psa_set_mapping?select=*"
    if game_id:
        url += f"&game_id=eq.{game_id}"
    # NOTE: psa_set_mapping is in shared schema
    headers = {**HEADERS, "Accept-Profile": "shared"}
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


async def get_cards_for_group(client: httpx.AsyncClient, group_id: int) -> list[dict]:
    """Fetch cards for a TCGplayer group (set) for matching."""
    url = (
        f"{REST_URL}/cards"
        f"?select=tcg_product_id,clean_name,name"
        f"&group_id=eq.{group_id}"
    )
    headers = {**HEADERS, "Accept-Profile": "shared"}
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


async def get_all_cards_for_game(client: httpx.AsyncClient, game_id: str) -> list[dict]:
    """Fetch ALL cards for a game. Used when we don't have group_id mapping."""
    all_cards = []
    offset = 0
    page_size = 1000
    headers = {**HEADERS, "Accept-Profile": "shared"}
    while True:
        url = (
            f"{REST_URL}/cards"
            f"?select=tcg_product_id,clean_name,name"
            f"&game_id=eq.{game_id}"
            f"&order=tcg_product_id"
            f"&offset={offset}&limit={page_size}"
        )
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        batch = resp.json()
        all_cards.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_cards


async def update_pop_data(client: httpx.AsyncClient, updates: list[dict]) -> int:
    """Batch update pop data on psa_arbitrage_opportunities + upsert full grade data to psa_pop_data.

    Each update dict: {tcg_product_id, psa9_pop, psa10_pop, total_pop, spec_id, grade_1..grade_10, etc.}
    Returns count of updated rows on arbitrage table.
    """
    now = datetime.now(timezone.utc).isoformat()
    updated = 0
    headers = {**HEADERS, "Content-Profile": "shared", "Accept-Profile": "shared"}

    # Collect full grade data for bulk upsert to psa_pop_data
    pop_rows = []

    import asyncio

    for i, u in enumerate(updates):
        psa10 = u["psa10_pop"]
        psa9 = u.get("psa9_pop", 0)
        total = u["total_pop"]
        rate = round(psa10 / total * 100, 2) if total > 0 else None
        body = {
            "psa9_pop": psa9,
            "psa10_pop": psa10,
            "total_pop": total,
            "psa10_rate": rate,
            "pop_fetched_at": now,
        }
        if u.get("spec_id"):
            body["psa_spec_id"] = u["spec_id"]

        url = f"{REST_URL}/psa_arbitrage_opportunities?tcg_product_id=eq.{u['tcg_product_id']}"

        # Retry up to 3 times with backoff on connection errors
        for attempt in range(3):
            try:
                resp = await client.patch(url, json=body, headers=headers)
                if resp.status_code < 300:
                    updated += 1
                else:
                    logger.warning(f"Pop update failed for tcg_id={u['tcg_product_id']}: {resp.status_code}")
                break
            except Exception as e:
                if attempt < 2:
                    wait = (attempt + 1) * 5
                    logger.warning(f"PATCH retry {attempt+1} for tcg_id={u['tcg_product_id']}: {e}, waiting {wait}s")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"PATCH failed after 3 attempts for tcg_id={u['tcg_product_id']}: {e}")

        # Small delay every 50 PATCHes to avoid overwhelming Supabase
        if (i + 1) % 50 == 0:
            await asyncio.sleep(1)

        # 2. Build psa_pop_data row with full grade distribution
        if u.get("spec_id"):
            pop_rows.append({
                "spec_id": u["spec_id"],
                "game_id": u.get("game_id", ""),
                "psa_set_id": u.get("psa_set_id", 0),
                "card_name": u.get("card_name", ""),
                "card_number": u.get("card_number", ""),
                "variant": u.get("variant", ""),
                "tcg_product_id": u["tcg_product_id"],
                "psa9_pop": u.get("psa9_pop", 0),
                "psa10_pop": psa10,
                "total_pop": total,
                "grade_authentic": u.get("grade_authentic", 0),
                "grade_1": u.get("grade_1", 0),
                "grade_1_5": u.get("grade_1_5", 0),
                "grade_2": u.get("grade_2", 0),
                "grade_2_5": u.get("grade_2_5", 0),
                "grade_3": u.get("grade_3", 0),
                "grade_3_5": u.get("grade_3_5", 0),
                "grade_4": u.get("grade_4", 0),
                "grade_4_5": u.get("grade_4_5", 0),
                "grade_5": u.get("grade_5", 0),
                "grade_5_5": u.get("grade_5_5", 0),
                "grade_6": u.get("grade_6", 0),
                "grade_6_5": u.get("grade_6_5", 0),
                "grade_7": u.get("grade_7", 0),
                "grade_7_5": u.get("grade_7_5", 0),
                "grade_8": u.get("grade_8", 0),
                "grade_8_5": u.get("grade_8_5", 0),
                "half_grade_total": u.get("half_grade_total", 0),
                "qualified_total": u.get("qualified_total", 0),
                "fetched_at": now,
            })

    # Bulk upsert full grade data to psa_pop_data
    if pop_rows:
        await _upsert_pop_data(client, pop_rows)

    return updated


async def _upsert_pop_data(client: httpx.AsyncClient, rows: list[dict]) -> int:
    """Upsert full grade distribution to psa_pop_data. ON CONFLICT(spec_id) update all fields."""
    import asyncio
    headers = {
        **HEADERS,
        "Content-Profile": "shared",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    url = f"{REST_URL}/psa_pop_data"
    upserted = 0
    for i in range(0, len(rows), 50):
        chunk = rows[i : i + 50]
        for attempt in range(3):
            try:
                resp = await client.post(url, json=chunk, headers=headers)
                if resp.status_code < 300:
                    upserted += len(chunk)
                else:
                    logger.warning(f"Pop data upsert failed: {resp.status_code} {resp.text[:200]}")
                break
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"Pop data upsert retry {attempt+1}: {e}")
                    await asyncio.sleep((attempt + 1) * 5)
                else:
                    logger.error(f"Pop data upsert failed after 3 attempts: {e}")
        await asyncio.sleep(0.5)  # Small delay between chunks
    logger.info(f"Upserted {upserted} rows to psa_pop_data")
    return upserted


async def get_spec_ids_for_game(client: httpx.AsyncClient, game_id: str) -> list[dict]:
    """Get spec_ids from psa_arbitrage_opportunities (cards we display in the PSA tab).

    NOTE: WARNING — Previously pulled from psa_pop_data (7,358 rows for pokemon).
    Playwright OOM'd on Render 512MB free tier trying to scrape all of them.
    Now pulls only cards with arbitrage opportunities (~46 for pokemon).
    """
    url = (
        f"{REST_URL}/psa_arbitrage_opportunities"
        f"?select=psa_spec_id,tcg_product_id,card_name,game_id"
        f"&game_id=eq.{game_id}"
        f"&psa_spec_id=gt.0"
    )
    headers = {**HEADERS, "Accept-Profile": "shared"}
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    # Rename psa_spec_id -> spec_id to match downstream expectations
    return [{"spec_id": r["psa_spec_id"], "tcg_product_id": r["tcg_product_id"],
             "card_name": r.get("card_name", ""), "game_id": r.get("game_id", game_id)}
            for r in resp.json()]


async def bridge_pop_data(client: httpx.AsyncClient) -> dict:
    """Call bridge_psa_pop_data() RPC to cross-match pop data from psa_pop_data into psa_arbitrage_opportunities.

    The PSA matcher and eBay matcher assign different tcg_product_ids for the same card.
    This RPC bridges the gap by matching on normalized card_name + card_number + game_id.
    """
    url = f"{REST_URL}/rpc/bridge_psa_pop_data"
    headers = {**HEADERS, "Content-Profile": "shared", "Accept-Profile": "shared"}
    resp = await client.post(url, json={}, headers=headers)
    if resp.status_code < 300:
        result = resp.json()
        logger.info(f"Bridge pop data: {result}")
        return result
    else:
        logger.warning(f"Bridge pop data RPC failed: {resp.status_code} {resp.text[:200]}")
        return {"error": resp.text[:200]}


async def write_sales_history(client: httpx.AsyncClient, sales: list[dict]) -> int:
    """Upsert sales entries to psa_sales_history. ON CONFLICT(spec_id, sold_at, price_cents, grade) skip."""
    import asyncio
    if not sales:
        return 0
    headers = {
        **HEADERS,
        "Content-Profile": "shared",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }
    url = f"{REST_URL}/psa_sales_history"
    upserted = 0
    for i in range(0, len(sales), 50):
        chunk = sales[i : i + 50]
        for attempt in range(3):
            try:
                resp = await client.post(url, json=chunk, headers=headers)
                if resp.status_code < 300:
                    upserted += len(chunk)
                else:
                    logger.warning(f"Sales upsert failed: {resp.status_code} {resp.text[:200]}")
                break
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"Sales upsert retry {attempt+1}: {e}")
                    await asyncio.sleep((attempt + 1) * 5)
                else:
                    logger.error(f"Sales upsert failed after 3 attempts: {e}")
        await asyncio.sleep(0.5)
    logger.info(f"Upserted {upserted} sales rows to psa_sales_history")
    return upserted


async def log_scrape(client: httpx.AsyncClient, result: dict) -> None:
    """Log scrape run to shared.cron_log following existing pattern."""
    url = f"{REST_URL}/cron_log"
    headers = {**HEADERS, "Content-Profile": "shared"}
    body = {
        "job_name": "psa_pop_scraper",
        "status": result.get("status", "success"),
        "duration_ms": result.get("duration_ms", 0),
        "detail": result,
        "started_at": result.get("started_at", datetime.now(timezone.utc).isoformat()),
    }
    await client.post(url, json=body, headers=headers)
