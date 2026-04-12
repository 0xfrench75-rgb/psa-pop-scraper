# @cnote[psa-scraper-no-ebay-item-id] WARNING - Do NOT add ebay_item_id until migration runs on prod.
"""PSA sales history scraper via JSON API (no browser needed).

Fetches sales data from PSA's internal API:
  GET /api/psa/researchJourney/spec/{specId}/salesHistory?pn=1&ps=50&g=&q=false&gt=ALL

NOTE: WARNING - PSA's WAF blocks curl_cffi on /api/ endpoints (TLS fingerprint detection).
Must use httpx with browser User-Agent instead. curl_cffi works for /Pop/GetSetItems
but NOT for /api/psa/researchJourney/. Discovered 2026-04-09.

NOTE: Context - The salesHistory API returns individual eBay sale records with
grade, price, date, sale type, cert number, and listing URL. No auth required,
just a browser User-Agent header via httpx.
"""

import asyncio
import logging
import httpx

from app.config import CRAWL_DELAY

logger = logging.getLogger(__name__)

SALES_API = "https://www.psacard.com/api/psa/researchJourney/spec/{spec_id}/salesHistory"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


async def scrape_sales_page(client: httpx.AsyncClient, spec_id: int) -> list[dict]:
    """Fetch sales history for a single spec_id via PSA JSON API.

    Returns list of dicts: {spec_id, grade, price_cents, sale_type, sold_at, auction_house}
    """
    # NOTE: Context - ps=50 fetches up to 50 sales per page. g= filters by grade (empty = all).
    # q=false means exclude qualified grades. gt=ALL means all grade types.
    url = SALES_API.format(spec_id=spec_id)
    params = {"pn": 1, "ps": 50, "g": "", "q": "false", "gt": "ALL"}

    try:
        resp = await client.get(url, params=params)
        if resp.status_code == 403:
            logger.warning(f"Spec {spec_id}: WAF 403 - blocked")
            return []
        if resp.status_code != 200:
            logger.error(f"Spec {spec_id}: HTTP {resp.status_code}")
            return []

        # Check for security challenge disguised as 200
        text = resp.text
        if "Security Check" in text[:500] or "Just a moment" in text[:500]:
            logger.warning(f"Spec {spec_id}: Security challenge in response")
            return []

        data = resp.json()
        total_count = data.get("totalCount", 0)
        raw_sales = data.get("sales", [])
        logger.info(f"Spec {spec_id}: API returned totalCount={total_count}, sales={len(raw_sales)}")

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
        # NOTE: Context - These fields were always available from PSA API but never stored.
        # cert_number + listing_url enable direct FK chain to ebay_sold_history.
        # ebay_item_id extractable from listing_url via regex /itm/(\d+) when column is added.
        "cert_number": str(entry["certNumber"]) if entry.get("certNumber") else None,
        "listing_url": entry.get("listingURL") or None,
        "image_url": entry.get("imageURL") or None,
    }


async def scrape_sales_batch(spec_ids: list[int], batch_size: int = 10) -> dict[int, list[dict]]:
    """Fetch sales history for multiple spec IDs using PSA JSON API.

    NOTE: WARNING - Must use httpx, NOT curl_cffi. PSA's WAF blocks curl_cffi's
    TLS fingerprint on /api/ endpoints. httpx with standard Python SSL passes fine.
    """
    results = {}
    total = len(spec_ids)

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=30,
    ) as client:
        for i, spec_id in enumerate(spec_ids):
            if i > 0:
                await asyncio.sleep(CRAWL_DELAY)

            sales = await scrape_sales_page(client, spec_id)
            results[spec_id] = sales
            logger.info(f"[{i+1}/{total}] spec {spec_id}: {len(sales)} sales (PSA 9/10)")

    return results
