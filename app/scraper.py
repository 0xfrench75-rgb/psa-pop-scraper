"""PSA pop data fetcher via /Pop/GetSetItems JSON API.

Two capabilities:
1. discover_all_sets() - crawls PSA's pop report site to find all TCG sets
2. scrape_sets() - fetches population data for a list of sets

Endpoint: POST https://www.psacard.com/Pop/GetSetItems
Params: headingID (set ID), categoryID ("156940" for TCG cards), length, start, draw
Returns: {data: [{SpecID, SubjectName, Variety, CardNumber, Grade10, GradeTotal, ...}]}
"""

import asyncio
import logging
import re
from curl_cffi.requests import AsyncSession

from app.config import CRAWL_DELAY

logger = logging.getLogger(__name__)

PSA_API_URL = "https://www.psacard.com/Pop/GetSetItems"
PSA_POP_BASE = "https://www.psacard.com/pop/tcg-cards"
TCG_CATEGORY_ID = "156940"

# Game keywords to filter PSA set slugs
GAME_KEYWORDS = {
    "sorcery": "sorcery",
    "pokemon": "pokemon",
    "one-piece": "one-piece",
    "dragon-ball-super": "dragon-ball",
}
# Languages to exclude (keep English only)
EXCLUDE_LANGS = [
    "japanese", "korean", "french", "german", "spanish", "italian",
    "portuguese", "chinese", "thai", "indonesian",
]
DISCOVERY_DELAY = 2.0  # Slower delay for discovery crawl (avoid rate limit)

# Hardcoded fallback sets when discovery crawl is blocked by Cloudflare.
# These are known PSA set IDs verified to return data via GetSetItems API.
# Updated: 2026-04-07
FALLBACK_SETS = [
    # Sorcery: Contested Realm
    {"game_id": "sorcery", "psa_set_id": 249139, "psa_set_slug": "sorcery-contested-realm-alpha", "psa_year": "2023"},
    {"game_id": "sorcery", "psa_set_id": 253551, "psa_set_slug": "sorcery-contested-realm-beta", "psa_year": "2023"},
    {"game_id": "sorcery", "psa_set_id": 285886, "psa_set_slug": "sorcery-contested-realm-arthurian-legends", "psa_year": "2024"},
    {"game_id": "sorcery", "psa_set_id": 274307, "psa_set_slug": "sorcery-contested-realm-dust-rewards", "psa_year": "2024"},
    {"game_id": "sorcery", "psa_set_id": 311705, "psa_set_slug": "sorcery-contested-realm-dust-rewards", "psa_year": "2025"},
]


async def discover_all_sets() -> list[dict]:
    """Crawl PSA's pop report site to discover all TCG card sets for our games.

    Returns list of dicts: {game_id, psa_set_id, psa_set_slug, psa_year}
    """
    all_sets = []
    async with AsyncSession() as session:
        # Step 1: Get year category IDs from the TCG cards index page
        try:
            resp = await session.get(f"{PSA_POP_BASE}/{TCG_CATEGORY_ID}", impersonate="chrome", timeout=30)
            if resp.status_code != 200:
                logger.error(f"Failed to fetch TCG index: {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch TCG index: {e}")
            return []

        year_cats = re.findall(r"/pop/tcg-cards/(20[2][0-9][^/]*)/(\d+)", resp.text)
        year_map = {}
        for y, cid in set(year_cats):
            year_map[y] = cid
        logger.info(f"Discovery: found {len(year_map)} year categories")

        # Step 2: For each year, find all sets matching our games
        for year in sorted(year_map.keys()):
            await asyncio.sleep(DISCOVERY_DELAY)
            cid = year_map[year]
            try:
                resp = await session.get(f"{PSA_POP_BASE}/{year}/{cid}", impersonate="chrome", timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"Discovery: failed year {year}: HTTP {resp.status_code}")
                    continue
            except Exception as e:
                logger.warning(f"Discovery: failed year {year}: {e}")
                continue

            # Extract set links: /pop/tcg-cards/{year}/{slug}/{setId}
            sets_found = re.findall(
                r"/pop/tcg-cards/" + re.escape(year) + r"/([^/\"]+)/(\d+)",
                resp.text,
            )

            for slug, sid in set(sets_found):
                lower = slug.lower()

                # Skip non-English
                if any(lang in lower for lang in EXCLUDE_LANGS):
                    continue

                # Match to our games
                for game_id, keyword in GAME_KEYWORDS.items():
                    if keyword in lower:
                        all_sets.append({
                            "game_id": game_id,
                            "psa_set_id": int(sid),
                            "psa_set_slug": slug,
                            "psa_year": year,
                        })
                        break

            logger.info(f"Discovery: year {year} -> {len(sets_found)} total, {len(all_sets)} ours so far")

    # Deduplicate by psa_set_id
    seen = set()
    unique = []
    for s in all_sets:
        if s["psa_set_id"] not in seen:
            seen.add(s["psa_set_id"])
            unique.append(s)

    logger.info(f"Discovery complete: {len(unique)} unique sets across {len(GAME_KEYWORDS)} games")
    return unique


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

    Input: SpecID, SubjectName, Variety, CardNumber, Grade1-Grade10, Grade1_5-Grade8_5,
           Grade1Q-Grade9Q, GradeN0, GradeTotal, HalfGradeTotal, QualifiedGradeTotal
    Output: full grade distribution (PSA 1-10, half grades, qualified, authentic)
    """
    results = []
    for c in raw_cards:
        results.append({
            "spec_id": c.get("SpecID", 0),
            "card_name": c.get("SubjectName", ""),
            "variant": c.get("Variety") or "",
            "card_number": c.get("CardNumber", ""),
            # Full grade distribution
            "grade_authentic": c.get("GradeN0", 0),
            "grade_1": c.get("Grade1", 0),
            "grade_1_5": c.get("Grade1_5", 0),
            "grade_2": c.get("Grade2", 0),
            "grade_2_5": c.get("Grade2_5", 0),
            "grade_3": c.get("Grade3", 0),
            "grade_3_5": c.get("Grade3_5", 0),
            "grade_4": c.get("Grade4", 0),
            "grade_4_5": c.get("Grade4_5", 0),
            "grade_5": c.get("Grade5", 0),
            "grade_5_5": c.get("Grade5_5", 0),
            "grade_6": c.get("Grade6", 0),
            "grade_6_5": c.get("Grade6_5", 0),
            "grade_7": c.get("Grade7", 0),
            "grade_7_5": c.get("Grade7_5", 0),
            "grade_8": c.get("Grade8", 0),
            "grade_8_5": c.get("Grade8_5", 0),
            "grade_9": c.get("Grade9", 0),
            "grade_10": c.get("Grade10", 0),
            # Totals
            "total_pop": c.get("GradeTotal", 0),
            "half_grade_total": c.get("HalfGradeTotal", 0),
            "qualified_total": c.get("QualifiedGradeTotal", 0),
            # Legacy aliases for existing code
            "psa9_pop": c.get("Grade9", 0),
            "psa10_pop": c.get("Grade10", 0),
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
