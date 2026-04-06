"""PSA pop data fetcher via /Pop/GetSetItems JSON API.

PSA's pop report pages use DataTables with server-side AJAX. The endpoint
returns structured JSON directly - no HTML parsing needed.

Endpoint: POST https://www.psacard.com/Pop/GetSetItems
Params: headingID (set ID), categoryID ("156940" for TCG cards), length, start, draw
Returns: {data: [{SpecID, SubjectName, Variety, CardNumber, Grade10, GradeTotal, ...}]}
"""

import asyncio
import logging
from curl_cffi.requests import AsyncSession

from app.config import CRAWL_DELAY

logger = logging.getLogger(__name__)

PSA_API_URL = "https://www.psacard.com/Pop/GetSetItems"
# NOTE: "156940" is the PSA category ID for all TCG cards. Hardcoded because
# it's the same for every TCG set on PSA's site.
TCG_CATEGORY_ID = "156940"


async def fetch_set_data(session: AsyncSession, psa_set_id: int) -> list[dict] | None:
    """Fetch population data for one PSA set via their JSON API.

    Paginates if set has more than PAGE_SIZE cards (some Pokemon sets have 500+).
    Returns list of card dicts with Grade10, GradeTotal, SubjectName, etc.
    Returns None on failure.
    """
    PAGE_SIZE = 500
    all_cards = []
    start = 0
    draw = 1

    try:
        while True:
            resp = await session.post(
                PSA_API_URL,
                impersonate="chrome",
                data={
                    "draw": draw,
                    "start": start,
                    "length": PAGE_SIZE,
                    "search": "",
                    "headingID": psa_set_id,
                    "categoryID": TCG_CATEGORY_ID,
                    "isPSADNA": "false",
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"PSA API returned {resp.status_code} for set {psa_set_id}")
                return None

            data = resp.json()
            cards = data.get("data", [])
            cards = [c for c in cards if c.get("SubjectName") != "TOTAL POPULATION"]
            all_cards.extend(cards)

            total = data.get("recordsTotal", 0)
            start += PAGE_SIZE
            draw += 1
            if start >= total:
                break
            await asyncio.sleep(CRAWL_DELAY)

        return all_cards

    except Exception as e:
        logger.error(f"Failed to fetch set {psa_set_id}: {e}")
        return None


def parse_cards(raw_cards: list[dict]) -> list[dict]:
    """Extract the fields we need from PSA's raw API response.

    Input fields: SpecID, SubjectName, Variety, CardNumber, Grade9, Grade10, GradeTotal
    Output fields: card_name, variant, card_number, psa9_pop, psa10_pop, total_pop, spec_id
    """
    results = []
    for c in raw_cards:
        results.append({
            "spec_id": c.get("SpecID", 0),
            "card_name": c.get("SubjectName", ""),
            "variant": c.get("Variety") or "",
            "card_number": c.get("CardNumber", ""),
            "psa9_pop": c.get("Grade9", 0),
            "psa10_pop": c.get("Grade10", 0),
            "total_pop": c.get("GradeTotal", 0),
        })
    return results


async def scrape_sets(sets: list[dict]) -> dict[int, list[dict]]:
    """Scrape multiple PSA sets with crawl delay between requests.

    Args:
        sets: list of psa_set_mapping rows with psa_set_id (and psa_set_slug for logging)

    Returns:
        dict mapping psa_set_id -> parsed card list
    """
    results = {}
    async with AsyncSession() as session:
        for i, s in enumerate(sets):
            if i > 0:
                await asyncio.sleep(CRAWL_DELAY)

            raw = await fetch_set_data(session, s["psa_set_id"])
            if raw is not None:
                cards = parse_cards(raw)
                results[s["psa_set_id"]] = cards
                logger.info(f"[{i+1}/{len(sets)}] {s.get('psa_set_slug', s['psa_set_id'])}: {len(cards)} cards")
            else:
                results[s["psa_set_id"]] = []
                logger.warning(f"[{i+1}/{len(sets)}] {s.get('psa_set_slug', s['psa_set_id'])}: FAILED")

    return results
