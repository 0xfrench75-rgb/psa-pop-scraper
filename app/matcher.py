"""Card name normalization and matching between PSA and our catalog.

PSA uses its own naming convention which may differ from TCGplayer.
Strategy: exact normalized match first, fuzzy fallback second.
"""

import re
import unicodedata
from thefuzz import fuzz

from app.config import MATCH_THRESHOLD

# PSA prefixes card names with these; TCGplayer suffixes them in parens.
# Strip both so "Full Art/Reshiram" and "Reshiram (Full Art)" both become "reshiram".
_VARIANT_PREFIXES = [
    "full art/", "secret rare/", "holo/", "reverse holo/", "promo/",
    "illustration rare/", "special art rare/", "ultra rare/",
]
_VARIANT_SUFFIXES_RE = re.compile(r"\s*\((?:full art|secret rare|holo|reverse holo|promo|illustration rare|special art rare|ultra rare)\)\s*$", re.IGNORECASE)


def normalize(name: str) -> str:
    """Lowercase, strip accents (NFD), remove variant prefixes/suffixes, remove punctuation."""
    # NFD decompose then strip combining marks (accents)
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    lower = stripped.lower().strip()

    # Strip PSA-style variant prefixes (e.g., "Full Art/Reshiram" -> "Reshiram")
    for prefix in _VARIANT_PREFIXES:
        if lower.startswith(prefix):
            lower = lower[len(prefix):]
            break

    # Strip TCGplayer-style variant suffixes (e.g., "Reshiram (Full Art)" -> "Reshiram")
    lower = _VARIANT_SUFFIXES_RE.sub("", lower)

    # Strip trailing card numbers like "001/165", "001", "SV001"
    # TCGplayer includes these in clean_name but PSA doesn't
    lower = re.sub(r"\s+\d{1,4}\s*/\s*\d{1,4}\s*$", "", lower)  # "001/165"
    lower = re.sub(r"\s+sv?\d{1,4}\s*$", "", lower)  # "SV001"
    lower = re.sub(r"\s+\d{1,4}\s*$", "", lower)  # trailing "001" or "0001"

    # Remove remaining punctuation, collapse whitespace
    cleaned = re.sub(r"[^a-z0-9\s]", "", lower)
    return re.sub(r"\s+", " ", cleaned).strip()


def build_lookup(cards: list[dict]) -> dict[str, int]:
    """Build {normalized_clean_name: tcg_product_id} from our DB cards.

    Uses clean_name (TCGplayer's cleaned name) as primary,
    falls back to name if clean_name is missing.
    """
    lookup = {}
    for card in cards:
        key = normalize(card.get("clean_name") or card.get("name", ""))
        if key:
            lookup[key] = card["tcg_product_id"]
    return lookup


def match_cards(
    scraped: list[dict], lookup: dict[str, int]
) -> tuple[list[dict], list[dict]]:
    """Match scraped PSA cards to our catalog.

    Returns (matched, unmatched) where matched cards have tcg_product_id added.
    """
    matched = []
    unmatched = []

    for card in scraped:
        norm_name = normalize(card["card_name"])

        # Exact match
        if norm_name in lookup:
            card["tcg_product_id"] = lookup[norm_name]
            matched.append(card)
            continue

        # Fuzzy match
        best_score = 0
        best_key = None
        for key in lookup:
            score = fuzz.ratio(norm_name, key)
            if score > best_score:
                best_score = score
                best_key = key

        if best_key and best_score >= MATCH_THRESHOLD:
            card["tcg_product_id"] = lookup[best_key]
            card["match_score"] = best_score
            card["matched_to"] = best_key
            matched.append(card)
        else:
            card["best_score"] = best_score
            card["best_match"] = best_key
            unmatched.append(card)

    return matched, unmatched
