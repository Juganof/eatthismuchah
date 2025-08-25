import re
from typing import Optional, Tuple


UNIT_ALIASES = {
    "g": {"g", "gram", "grams", "gr", "gr."},
    "kg": {"kg", "kilogram", "kilo"},
    "ml": {"ml", "milliliter", "milliliters"},
    "l": {"l", "liter", "liters"},
    "el": {"el", "eetlepel", "eetlepels", "tbsp"},
    "tl": {"tl", "theelepel", "theelepels", "tsp"},
    "st": {"st", "stuk", "stuks", "stuk(s)", "stukje", "stukjes"},
    "blik": {"blik", "blikje", "blikjes"},
    "pak": {"pak", "pakje", "pakjes"},
}


def normalize_unit(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip().lower()
    for key, vals in UNIT_ALIASES.items():
        if u in vals:
            return key
    return u


def parse_quantity_unit_name(raw: str) -> Tuple[str, Optional[float], Optional[str]]:
    """Parse a Dutch ingredient line into (name, quantity, unit).

    Handles patterns like:
      - "200 g kipfilet"
      - "2 el olijfolie"
      - "1 blikje tomatenblokjes (400 g)" -> quantity=1, unit='blik'
      - "1-2 tenen knoflook" -> quantity=1.5, unit='st'
      - "500 ml water"
    Falls back to name=raw if no match.
    """
    text = (raw or "").strip()
    if not text:
        return raw, None, None

    # Replace commas in numbers (e.g., 1,5) with dots
    t = re.sub(r"(\d),(\d)", r"\1.\2", text)

    # Range like 1-2 -> average
    m = re.match(r"^(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)[^\w]?\s*(.*)$", t)
    if m:
        q = (float(m.group(1)) + float(m.group(2))) / 2.0
        rest = m.group(3).strip()
        # optional unit next
        m2 = re.match(r"^(\w+)\b\s*(.*)$", rest)
        unit = None
        name = rest
        if m2:
            unit = normalize_unit(m2.group(1))
            name = m2.group(2).strip() or rest
        return name or raw, q, unit

    # Quantity + unit at start
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([A-Za-zÀ-ÿ\.]+)\b\s*(.*)$", t)
    if m:
        q = float(m.group(1))
        unit = normalize_unit(m.group(2).rstrip('.'))
        name = m.group(3).strip()
        return name or raw, q, unit

    # Quantity only -> pieces
    m = re.match(r"^(\d+(?:\.\d+)?)\s+(.*)$", t)
    if m:
        q = float(m.group(1))
        name = m.group(2).strip()
        return name or raw, q, "st"

    # Fallback: try parentheses for size hints but keep as name
    return text, None, None


def unit_to_grams(quantity: float, unit: Optional[str]) -> Optional[float]:
    if quantity is None or unit is None:
        return None
    u = normalize_unit(unit)
    if u == "g":
        return quantity
    if u == "kg":
        return quantity * 1000.0
    if u == "ml":
        # Approximate density 1g/ml by default
        return quantity * 1.0
    if u == "l":
        return quantity * 1000.0
    if u == "el":
        return quantity * 15.0  # tablespoon ~15ml
    if u == "tl":
        return quantity * 5.0   # teaspoon ~5ml
    if u in {"st", "blik", "pak"}:
        # Unknown weight per piece/pack; require manual mapping later
        return None
    return None

