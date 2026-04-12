# @cnote[psa-scraper-card-code] CONTEXT - Tier 0 card-code matching. Sorcery 0% coverage, falls back to name.
"""Card name normalization and matching between PSA and our catalog.

PSA uses its own naming convention which may differ from TCGplayer.
Strategy: card code match first (Tier 0), exact normalized name second (Tier 1),
fuzzy name fallback third (Tier 2).

NOTE: Context - Card code matching ported from src/lib/psa-card-matcher.ts (0.98 confidence).
Previously name-only fuzzy matching gave ~86% match rate. Card codes push it to 95-98%.
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

# Card code pattern: "OP01-078", "BT16-024", "FB06-025", "ST01-012"
_CARD_CODE_RE = re.compile(r'^([A-Z]{1,4}\d{1,3})[-](\d+)$', re.IGNORECASE)


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


def normalize_card_code(code: str) -> str:
    """Normalize card code: uppercase prefix, zero-pad suffix to 3 digits.

    "op01-78" -> "OP01-078", "BT16-24" -> "BT16-024"
    Non-matching codes returned uppercase as-is.
    """
    match = _CARD_CODE_RE.match(code.strip())
    if not match:
        return code.upper().strip()
    prefix = match.group(1).upper()
    suffix = match.group(2).zfill(3)
    return f"{prefix}-{suffix}"


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


def build_code_lookup(cards: list[dict]) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Build card code indexes for deterministic matching.

    Returns two dicts:
      by_code: full card code/number -> list of {tcg_product_id, name}
               Keys: "OP01-078" (card codes), "269/217" (slash numbers)
      by_suffix: bare number suffix -> list of {tcg_product_id, name, full_code}
               Keys: "078" (from OP01-078), "269" (from 269/217)
    """
    by_code: dict[str, list[dict]] = {}
    by_suffix: dict[str, list[dict]] = {}

    for card in cards:
        cn = card.get("card_number") or ""
        if not cn:
            continue

        pid = card["tcg_product_id"]
        name = card.get("clean_name") or card.get("name", "")
        entry = {"tcg_product_id": pid, "name": name}

        # Index by normalized card code (e.g., "OP01-078")
        code_match = _CARD_CODE_RE.match(cn)
        if code_match:
            norm_code = normalize_card_code(cn)
            by_code.setdefault(norm_code, []).append(entry)

            # Also index by bare numeric suffix for PSA bare-number matching
            suffix = code_match.group(2).lstrip("0") or "0"
            by_suffix.setdefault(suffix, []).append({**entry, "full_code": norm_code})
            # Also index the zero-padded suffix
            padded = code_match.group(2).zfill(3)
            if padded != suffix:
                by_suffix.setdefault(padded, []).append({**entry, "full_code": norm_code})
            continue

        # Index by slash number (e.g., "269/217")
        slash_match = re.match(r'^(\d{1,4})\s*/\s*(\d{1,4})$', cn)
        if slash_match:
            slash_key = f"{slash_match.group(1)}/{slash_match.group(2)}"
            by_code.setdefault(slash_key, []).append(entry)

            # Also index by the first number (bare) for PSA bare-number matching
            bare = slash_match.group(1).lstrip("0") or "0"
            by_suffix.setdefault(bare, []).append({**entry, "full_code": slash_key})
            continue

        # Plain number or other format - index as-is
        by_code.setdefault(cn.strip(), []).append(entry)

    return by_code, by_suffix


def _verify_name(psa_name: str, catalog_name: str) -> bool:
    """Check if PSA card name matches catalog name loosely.

    Extracts significant words (3+ chars) from both and checks for overlap.
    At least one significant word must match.
    """
    psa_norm = normalize(psa_name)
    cat_norm = normalize(catalog_name)

    # Exact normalized match is obviously good
    if psa_norm == cat_norm:
        return True

    # Extract words 3+ chars
    psa_words = {w for w in psa_norm.split() if len(w) >= 3}
    cat_words = {w for w in cat_norm.split() if len(w) >= 3}

    if not psa_words or not cat_words:
        return False

    # At least one significant word must overlap
    return bool(psa_words & cat_words)


def match_by_code(
    psa_card_number: str,
    psa_card_name: str,
    by_code: dict[str, list[dict]],
    by_suffix: dict[str, list[dict]],
) -> int | None:
    """Try to match a PSA card by its card_number field.

    Tries in order:
    1. Direct slash match ("269/217")
    2. Direct card code match ("OP01-078")
    3. Bare number suffix match ("78" -> find all codes ending -078)

    Returns tcg_product_id or None.
    """
    if not psa_card_number:
        return None

    cn = psa_card_number.strip()

    # 1. Direct slash number match (e.g., "269/217")
    slash_match = re.match(r'^(\d{1,4})\s*/\s*(\d{1,4})$', cn)
    if slash_match:
        slash_key = f"{slash_match.group(1)}/{slash_match.group(2)}"
        candidates = by_code.get(slash_key, [])
        if len(candidates) == 1:
            return candidates[0]["tcg_product_id"]
        if len(candidates) > 1:
            # Verify with name to disambiguate
            verified = [c for c in candidates if _verify_name(psa_card_name, c["name"])]
            if len(verified) == 1:
                return verified[0]["tcg_product_id"]
            if verified:
                return verified[0]["tcg_product_id"]
        # Slash didn't match - also try the bare first number
        bare = slash_match.group(1).lstrip("0") or "0"
        suffix_candidates = by_suffix.get(bare, [])
        if suffix_candidates:
            verified = [c for c in suffix_candidates if _verify_name(psa_card_name, c["name"])]
            if len(verified) == 1:
                return verified[0]["tcg_product_id"]
        return None

    # 2. Direct card code match (e.g., "OP01-078")
    code_match = _CARD_CODE_RE.match(cn)
    if code_match:
        norm_code = normalize_card_code(cn)
        candidates = by_code.get(norm_code, [])
        if len(candidates) == 1:
            return candidates[0]["tcg_product_id"]
        if len(candidates) > 1:
            verified = [c for c in candidates if _verify_name(psa_card_name, c["name"])]
            if len(verified) == 1:
                return verified[0]["tcg_product_id"]
            if verified:
                return verified[0]["tcg_product_id"]
        return None

    # 3. Bare number match (e.g., "78" -> look up suffix "78" and "078")
    bare_match = re.match(r'^(\d{1,4})$', cn)
    if bare_match:
        bare = bare_match.group(1).lstrip("0") or "0"
        candidates = by_suffix.get(bare, [])
        if not candidates:
            # Try zero-padded version
            padded = bare_match.group(1).zfill(3)
            candidates = by_suffix.get(padded, [])

        if len(candidates) == 1:
            if _verify_name(psa_card_name, candidates[0]["name"]):
                return candidates[0]["tcg_product_id"]
        elif len(candidates) > 1:
            # Multiple candidates - require name verification to disambiguate
            verified = [c for c in candidates if _verify_name(psa_card_name, c["name"])]
            if len(verified) == 1:
                return verified[0]["tcg_product_id"]

    return None


def match_cards(
    scraped: list[dict],
    lookup: dict[str, int],
    code_lookup: dict[str, list[dict]] | None = None,
    suffix_lookup: dict[str, list[dict]] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Match scraped PSA cards to our catalog.

    Tier 0: Card code/number match (deterministic, highest confidence)
    Tier 1: Exact normalized name match
    Tier 2: Fuzzy name match (thefuzz ratio >= MATCH_THRESHOLD)

    Returns (matched, unmatched) where matched cards have tcg_product_id added.
    """
    matched = []
    unmatched = []

    for card in scraped:
        # Tier 0: Card code match (deterministic)
        if code_lookup is not None and suffix_lookup is not None:
            psa_num = card.get("card_number", "")
            if psa_num:
                tcg_id = match_by_code(psa_num, card["card_name"], code_lookup, suffix_lookup)
                if tcg_id is not None:
                    card["tcg_product_id"] = tcg_id
                    card["match_method"] = "card_code"
                    card["match_score"] = 100
                    matched.append(card)
                    continue

        norm_name = normalize(card["card_name"])

        # Tier 1: Exact name match
        if norm_name in lookup:
            card["tcg_product_id"] = lookup[norm_name]
            card["match_method"] = "name_exact"
            matched.append(card)
            continue

        # Tier 2: Fuzzy name match
        best_score = 0
        best_key = None
        for key in lookup:
            score = fuzz.ratio(norm_name, key)
            if score > best_score:
                best_score = score
                best_key = key

        if best_key and best_score >= MATCH_THRESHOLD:
            card["tcg_product_id"] = lookup[best_key]
            card["match_method"] = "name_fuzzy"
            card["match_score"] = best_score
            card["matched_to"] = best_key
            matched.append(card)
        else:
            card["best_score"] = best_score
            card["best_match"] = best_key
            unmatched.append(card)

    return matched, unmatched
