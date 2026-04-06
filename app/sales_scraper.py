"""PSA sales history scraper via Playwright (headless Chromium).

Fetches spec pages at /spec/psa/{specId} and extracts sales history entries.
Each entry has: grade, price, sale type, date, source.

Requires Playwright because PSA's spec page is a Next.js App Router site
with client-rendered sales data (React Server Actions). curl_cffi only gets
the HTML shell without sales entries.

Memory: ~250-300 MB (Chromium + Python). Fits Render free tier (512 MB)
if we process one page at a time and close between batches.
"""

import asyncio
import logging
import re
from datetime import datetime
from playwright.async_api import async_playwright, Browser

from app.config import CRAWL_DELAY

logger = logging.getLogger(__name__)

# Chrome user agent to bypass Cloudflare
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


async def scrape_sales_page(page, spec_id: int) -> list[dict]:
    """Scrape sales history from a single PSA spec page.

    Returns list of dicts: {grade, price_cents, sale_type, sold_at, source}
    """
    url = f"https://www.psacard.com/spec/psa/{spec_id}"
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if resp.status != 200:
            logger.error(f"Spec page {spec_id}: HTTP {resp.status}")
            return []

        # Wait for React to hydrate and render sales
        await page.wait_for_timeout(5000)

        # Check for Cloudflare block
        content = await page.content()
        if "Just a moment" in content[:500]:
            logger.error(f"Spec page {spec_id}: Cloudflare blocked")
            return []

        # Extract sales entries from DOM
        raw_sales = await page.evaluate("""() => {
            const buttons = document.querySelectorAll("button[aria-label='View Details']");
            return Array.from(buttons).map(btn => {
                const ps = btn.querySelectorAll('p');
                return Array.from(ps).map(p => p.textContent.trim());
            });
        }""")

        # Parse raw text arrays into structured data
        sales = []
        for entry in raw_sales:
            parsed = _parse_sale_entry(entry, spec_id)
            if parsed:
                sales.append(parsed)

        return sales

    except Exception as e:
        logger.error(f"Failed to scrape spec {spec_id}: {e}")
        return []


def _parse_sale_entry(texts: list[str], spec_id: int) -> dict | None:
    """Parse a sale entry from DOM text array.

    Expected format: ['PSA 10', 'eBay · Auction', 'Apr 6, 2026', '$436.00']
    """
    if len(texts) < 4:
        return None

    grade = texts[0]  # e.g., "PSA 10", "PSA 9"
    source_type = texts[1]  # e.g., "eBay · Auction"
    date_str = texts[2]  # e.g., "Apr 6, 2026"
    price_str = texts[3]  # e.g., "$436.00"

    # Filter: only PSA 9 and PSA 10
    if grade not in ("PSA 9", "PSA 10"):
        return None

    # Parse price
    price_match = re.search(r"\$?([\d,]+)\.(\d{2})", price_str)
    if not price_match:
        return None
    dollars = int(price_match.group(1).replace(",", ""))
    cents = int(price_match.group(2))
    price_cents = dollars * 100 + cents

    # Parse source and sale type
    source = "eBay"
    sale_type = "Unknown"
    if "·" in source_type:
        parts = source_type.split("·")
        source = parts[0].strip()
        sale_type = parts[1].strip()

    # Parse date
    sold_at = None
    try:
        sold_at = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            sold_at = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            sold_at = date_str

    return {
        "spec_id": spec_id,
        "grade": grade,
        "price_cents": price_cents,
        "sale_type": sale_type,
        "sold_at": sold_at,
        "source": source,
    }


async def scrape_sales_batch(spec_ids: list[int], batch_size: int = 10) -> dict[int, list[dict]]:
    """Scrape sales history for multiple spec IDs using Playwright.

    Opens browser, processes batch_size pages, closes browser to free memory.
    Returns dict mapping spec_id -> list of sales entries.
    """
    results = {}
    total = len(spec_ids)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
            ],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 720},
        )
        page = await context.new_page()

        for i, spec_id in enumerate(spec_ids):
            if i > 0:
                await asyncio.sleep(CRAWL_DELAY)

            sales = await scrape_sales_page(page, spec_id)
            results[spec_id] = sales
            logger.info(f"[{i+1}/{total}] spec {spec_id}: {len(sales)} sales (PSA 9/10)")

            # Close and reopen page every batch_size pages to prevent memory bloat
            if (i + 1) % batch_size == 0 and i + 1 < total:
                await page.close()
                page = await context.new_page()
                logger.info(f"  Page recycled at {i+1}/{total}")

        await browser.close()

    return results
