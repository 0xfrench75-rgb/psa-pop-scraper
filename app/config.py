import os

# Supabase - required in production, optional for local testing
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Auth for incoming requests
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "dev-key")

# Scraping
CRAWL_DELAY = 1.0  # seconds between requests per robots.txt

# Matching
MATCH_THRESHOLD = 85  # thefuzz ratio minimum for fuzzy match
