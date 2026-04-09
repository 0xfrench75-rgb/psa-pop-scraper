"""PSA sales history scraper via JSON API (no browser needed).

Fetches sales data from PSA's internal API:
  GET /api/psa/researchJourney/spec/{specId}/salesHistory?pn=1&ps=50&g=&q=false&gt=ALL

NOTE: WARNING - Previously used Playwright to render PSA spec pages and extract
sales from DOM. Playwright + Chromium used ~300MB and OOM'd on Render 512MB free tier.
Discovered the JSON API on 2026-04-09 by intercepting network requests.
This approach uses curl_cffi (~5MB) instead of Chromium (~300MB).

NOTE: Context - The salesHistory API returns individual eBay sale records with
grade, price, date, sale type, cert number, and listing URL. No auth required,
just a browser User-Agent header.
"""

import asyncio
import logging
from curl_cffi.requests import AsyncSession

from app.config import CRAWL_DELAY

logger = logging.getLogger(__name__)

SALES_API = "https://www.psacard.com/api/psa/researchJourney/spec/{spec_id}/salesHistory"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


async def scrape_sales_page(session: AsyncSession, spec_id: int) -> list[dict]:
    """Fetch sales history for a single spec_id via PSA JSON API.

    Returns list of dicts: {spec_id, grade, price_cents, sale_type, sold_at, auction_house}
    """
    # NOTE: Context - ps=50 fetches up to 50 sales per page. g= filters by grade (empty = all).
    # q=false means exclude qualified grades. gt=ALL means all grade types.
    url = SALES_API.format(spec_id=spec_id)
    params = {"pn": 1, "ps": 50, "g": "", "q": "false", "gt": "ALL"}

    try:
        resp = await session.get(url, params=params, impersonate="chrome")
        if resp.status_code == 403:
            logger.warning(f"Spec {spec_id}: Cloudflare 403 - may need rate limit adjustment")
            return []
        if resp.status_code != 200:
            logger.error(f"Spec {spec_id}: HTTP {resp.status_code}")
            return []

        data = resp.json()
        raw_sales = data.get("sales", [])

        sales = []
        for entry in raw_sales:
            parsed = _parse_api_sale(entry, spec_id)
            if parsed:
                sales.append(parsed)

        return sales

    except Exception as e:
        logger.error(f"Failed to fetch sales for spec {spec_id}: {e}")
        return []


def _parse_api_sale(entry: dict, spec_id: int) -> dict | None:
    """Parse a sale entry from PSA JSON API response.

    API returns: {saleItemId, specId, certNumber, auctionHouse, saleDate,
                  saleType, salePrice, gradeValue, lotNumber, listingURL, ...}
    """
    grade_value = entry.get("gradeValue")
    if grade_value is None:
        return None

    # Only keep PSA 9 and PSA 10
    if grade_value not in (9, 10):
        return None

    sale_price = entry.get("salePrice")
    if sale_price is None or sale_price <= 0:
        return None

    # Convert dollars to cents
    price_cents = int(round(float(sale_price) * 100))

    # Parse date (ISO format from API: "2026-04-08T23:13:00.000Z")
    sale_date = entry.get("saleDate", "")
    sold_at = sale_date[:10] if sale_date else None  # Extract YYYY-MM-DD

    return {
        "spec_id": spec_id,
        "grade": f"PSA {grade_value}",
        "price_cents": price_cents,
        "sale_type": entry.get("saleType", "Unknown"),
        "sold_at": sold_at,
        "auction_house": entry.get("auctionHouse", "eBay"),
    }


async def scrape_sales_batch(spec_ids: list[int], batch_size: int = 10) -> dict[int, list[dict]]:
    """Fetch sales history for multiple spec IDs using PSA JSON API.

    NOTE: Context - Uses curl_cffi with Chrome TLS impersonation (same as pop scraper).
    No browser needed. ~5MB memory vs ~300MB for Playwright.
    """
    results = {}
    total = len(spec_ids)

    async with AsyncSession(impersonate="chrome") as session:
        for i, spec_id in enumerate(spec_ids):
            if i > 0:
                await asyncio.sleep(CRAWL_DELAY)

            sales = await scrape_sales_page(session, spec_id)
            results[spec_id] = sales
            logger.info(f"[{i+1}/{total}] spec {spec_id}: {len(sales)} sales (PSA 9/10)")

    return results
