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

    for u in updates:
        # 1. PATCH arbitrage table (legacy - psa9, psa10, total only)
        url = f"{REST_URL}/psa_arbitrage_opportunities?tcg_product_id=eq.{u['tcg_product_id']}"
        psa10 = u["psa10_pop"]
        total = u["total_pop"]
        rate = round(psa10 / total * 100, 2) if total > 0 else None
        body = {
            "psa9_pop": u.get("psa9_pop"),
            "psa10_pop": psa10,
            "total_pop": total,
            "psa10_rate": rate,
            "pop_fetched_at": now,
        }
        if u.get("spec_id"):
            body["psa_spec_id"] = u["spec_id"]
        resp = await client.patch(url, json=body, headers=headers)
        if resp.status_code < 300:
            updated += 1
        else:
            logger.warning(f"Pop update failed for tcg_id={u['tcg_product_id']}: {resp.status_code}")

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
    headers = {
        **HEADERS,
        "Content-Profile": "shared",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    url = f"{REST_URL}/psa_pop_data"
    upserted = 0
    for i in range(0, len(rows), 50):
        chunk = rows[i : i + 50]
        resp = await client.post(url, json=chunk, headers=headers)
        if resp.status_code < 300:
            upserted += len(chunk)
        else:
            logger.warning(f"Pop data upsert failed: {resp.status_code} {resp.text[:200]}")
    logger.info(f"Upserted {upserted} rows to psa_pop_data")
    return upserted


async def get_spec_ids_for_game(client: httpx.AsyncClient, game_id: str) -> list[dict]:
    """Get spec_ids from psa_pop_data for a game (cards we've already scraped pop for)."""
    url = (
        f"{REST_URL}/psa_pop_data"
        f"?select=spec_id,tcg_product_id,card_name,game_id"
        f"&game_id=eq.{game_id}"
        f"&spec_id=gt.0"
    )
    headers = {**HEADERS, "Accept-Profile": "shared"}
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


async def upsert_sales_history(client: httpx.AsyncClient, sales: list[dict]) -> int:
    """Insert sales history rows into psa_sales_history.

    Uses upsert (ON CONFLICT DO NOTHING) to avoid duplicates on (spec_id, sold_at, price_cents, grade).
    Returns count of inserted rows.
    """
    if not sales:
        return 0
    url = f"{REST_URL}/psa_sales_history"
    headers = {
        **HEADERS,
        "Content-Profile": "shared",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }
    now = datetime.now(timezone.utc).isoformat()
    rows = [{**s, "fetched_at": now} for s in sales]

    # Insert in chunks of 50
    inserted = 0
    for i in range(0, len(rows), 50):
        chunk = rows[i : i + 50]
        resp = await client.post(url, json=chunk, headers=headers)
        if resp.status_code < 300:
            inserted += len(chunk)
        else:
            logger.warning(f"Sales insert failed: {resp.status_code} {resp.text[:200]}")
    return inserted


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
