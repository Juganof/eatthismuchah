import csv
import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

from .http import fetch
from .db import upsert_product, mark_seen_page, is_seen


def import_products_from_json(data: Iterable[Dict]) -> List[Dict]:
    """Normalize products from a JSON iterable into DB-ready rows.

    Expected fields (best-effort):
    - ah_id, name, brand, category, unit, price_eur
    - kcal_per_100, protein_g_per_100, carbs_g_per_100, fat_g_per_100, fiber_g_per_100, salt_g_per_100
    - url, image_url
    You can provide any subset; missing fields remain null.
    """
    now = datetime.utcnow().isoformat()
    rows: List[Dict] = []
    for p in data:
        row = {
            "ah_id": p.get("ah_id") or p.get("id"),
            "name": p.get("name"),
            "brand": p.get("brand"),
            "category": p.get("category"),
            "unit": p.get("unit"),
            "price_eur": _to_float(p.get("price_eur")),
            "kcal_per_100": _to_float(p.get("kcal_per_100")),
            "protein_g_per_100": _to_float(p.get("protein_g_per_100")),
            "carbs_g_per_100": _to_float(p.get("carbs_g_per_100")),
            "fat_g_per_100": _to_float(p.get("fat_g_per_100")),
            "fiber_g_per_100": _to_float(p.get("fiber_g_per_100")),
            "salt_g_per_100": _to_float(p.get("salt_g_per_100")),
            "nutrition_json": json.dumps(p.get("nutrition")) if p.get("nutrition") else None,
            "url": p.get("url"),
            "image_url": p.get("image_url"),
            "last_seen": now,
        }
        rows.append(row)
    return rows


def import_products_from_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return import_products_from_json(reader)


def _to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def scrape_ah_product_page(url: str) -> Dict:
    """Stub for personal scraping of an AH product page.

    For personal use only. The AH site may render data client-side and change often.
    Consider using your browser devtools to capture the underlying JSON calls
    the page makes (e.g., API endpoints) and save them to a JSON file for import.

    Example flow:
    - Open product page in your browser
    - In Network tab, find the JSON response containing nutrition and product info
    - Save as JSON and feed into import_products_from_json()
    """
    status, data = fetch(url, delay_s=1.0)
    html = data.decode("utf-8", errors="ignore")
    # Try JSON-LD Product first
    m = re.search(r"<script[^>]+type=\"application/ld\+json\"[^>]*>(.*?)</script>", html, re.I | re.S)
    product = None
    if m:
        try:
            j = json.loads(m.group(1))
            if isinstance(j, list):
                for obj in j:
                    if obj.get("@type") == "Product":
                        product = obj
                        break
            elif isinstance(j, dict) and (j.get("@type") == "Product" or (isinstance(j.get("@type"), list) and "Product" in j.get("@type"))):
                product = j
        except Exception:
            product = None
    # Try Next.js __NEXT_DATA__ as fallback
    if product is None:
        m2 = re.search(r"<script id=\"__NEXT_DATA__\" type=\"application/json\">(.*?)</script>", html, re.I | re.S)
        if m2:
            try:
                next_data = json.loads(m2.group(1))
                # Attempt to directly extract AH product fields and normalize
                row = _extract_ah_product_row_from_next(next_data, url)
                if row:
                    # Ensure name fallback from HTML if missing
                    if not row.get("name"):
                        row["name"] = _extract_name_from_html(html)
                    if not row.get("image_url"):
                        row["image_url"] = _extract_image_from_html(html)
                    return row
                # Fallback to return raw next_data to generic normalizer
                product = _extract_product_from_next(next_data)
            except Exception:
                product = None
    if product is None:
        raise ValueError("Could not locate product data in page")
    row = _normalize_product(product, url)
    # Fill missing name/image via HTML fallbacks
    if not row.get("name"):
        row["name"] = _extract_name_from_html(html)
    if not row.get("image_url"):
        row["image_url"] = _extract_image_from_html(html)
    return row


def _extract_product_from_next(next_data: Dict) -> Dict:
    # Keep generic fallback, but prefer scanning in _extract_ah_product_row_from_next
    return next_data


def _extract_ah_product_row_from_next(next_data: Dict[str, Any], url: str) -> Optional[Dict]:
    """Attempt to extract name/brand/nutrition from AH Next.js data.

    This scans nested dict/list structures for keys that look like product info
    and nutrition (per 100g/ml) and constructs a normalized product row.
    """
    name, brand = _scan_for_name_brand(next_data)
    nutrit = _scan_for_nutrition(next_data)
    if not (name or nutrit):
        return None
    row = {
        "ah_id": _extract_ah_id_from_url(url),
        "name": name,
        "brand": brand,
        "category": None,
        "unit": None,
        "price_eur": None,
        "kcal_per_100": nutrit.get("kcal"),
        "protein_g_per_100": nutrit.get("protein_g"),
        "carbs_g_per_100": nutrit.get("carbs_g"),
        "fat_g_per_100": nutrit.get("fat_g"),
        "fiber_g_per_100": nutrit.get("fiber_g"),
        "salt_g_per_100": nutrit.get("salt_g"),
        "nutrition_json": json.dumps(next_data),
        "url": url,
        "image_url": None,
        "last_seen": datetime.utcnow().isoformat(),
    }
    return row


def _extract_ah_id_from_url(url: str) -> Optional[str]:
    # Typical pattern: /producten/product/wiXXXXX/ or with slug segments
    m = re.search(r"/producten/product/([^/?#]+)", url)
    if m:
        seg = m.group(1)
        # Take alnum token that looks like an id (starts with wi or digits)
        m2 = re.match(r"(wi[a-zA-Z0-9]+|\d{6,14})", seg)
        return m2.group(1) if m2 else seg
    return None


def _scan_for_name_brand(obj: Any) -> Tuple[Optional[str], Optional[str]]:
    name = None
    brand = None
    # BFS over nested dict/list
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            # Prefer explicit fields first
            if not name:
                for k in ("title", "name", "productTitle", "displayName"):
                    if k in cur and isinstance(cur[k], str) and cur[k].strip():
                        name = cur[k].strip()
                        break
            if not brand:
                for k in ("brand", "brandName", "brandname"):
                    v = cur.get(k)
                    if isinstance(v, dict):
                        b = v.get("name") or v.get("value")
                        if isinstance(b, str) and b.strip():
                            brand = b.strip()
                            break
                    elif isinstance(v, str) and v.strip():
                        brand = v.strip()
                        break
            # enqueue values
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            stack.extend([v for v in cur if isinstance(v, (dict, list))])
        if name and brand:
            break
    return name, brand


def _scan_for_nutrition(obj: Any) -> Dict[str, Optional[float]]:
    """Scan nested object for nutrition per 100g/ml.

    Heuristics: prefer keys mentioning 100 (per100, per_100, 100g/ml). Fallback to any matching keys.
    """
    candidates: List[Tuple[str, float]] = []
    def walk(o: Any, path: str = ""):
        if isinstance(o, dict):
            for k, v in o.items():
                kp = f"{path}.{k}" if path else str(k)
                if isinstance(v, (dict, list)):
                    walk(v, kp)
                else:
                    val = _norm_float(v)
                    if val is not None:
                        candidates.append((kp.lower(), val))
        elif isinstance(o, list):
            for i, v in enumerate(o):
                walk(v, f"{path}[{i}]")
    walk(obj)
    def pick(keys: List[str]) -> Optional[float]:
        best_key = None
        best_val = None
        # Two passes: prefer '100' in key path
        for prefer_100 in (True, False):
            for k, v in candidates:
                if any(key in k for key in keys):
                    if prefer_100 and not ("100" in k or "per100" in k or "per_100" in k or "100g" in k or "100ml" in k):
                        continue
                    best_key, best_val = k, v
                    break
            if best_val is not None:
                break
        return best_val
    out = {
        "kcal": pick(["kcal", "calorie", "energykcal", "energy_kcal", "energy.kcal"]),
        "protein_g": pick(["protein", "proteins", "eiwit"]),
        "carbs_g": pick(["carbo", "carb", "carbohydrates", "koolhyd"]),
        "fat_g": pick(["fat", "fats", "vet"]),
        "fiber_g": pick(["fiber", "fibre", "vezel"]),
        "salt_g": pick(["salt", "sodium", "zout"]),
    }
    return out


def _norm_float(v):
    try:
        return float(re.search(r"([0-9]+(?:\.[0-9]+)?)", str(v)).group(1))
    except Exception:
        return None


def _normalize_product(prod: Dict, url: str) -> Dict:
    # Attempt to unify Product JSON-LD or Next.js data structures.
    name = prod.get("name") if isinstance(prod, dict) else None
    brand = None
    if isinstance(prod, dict):
        b = prod.get("brand")
        if isinstance(b, dict):
            brand = b.get("name")
        elif isinstance(b, str):
            brand = b
    # Nutrition can be in JSON-LD or nested (Next.js). Try both
    kcal = protein = carbs = fat = fiber = salt = None
    if isinstance(prod, dict):
        nutrition = prod.get("nutrition")
        if isinstance(nutrition, dict):
            kcal = _norm_float(nutrition.get("calories"))
            protein = _norm_float(nutrition.get("proteinContent"))
            carbs = _norm_float(nutrition.get("carbohydrateContent"))
            fat = _norm_float(nutrition.get("fatContent"))
            fiber = _norm_float(nutrition.get("fiberContent"))
            salt = _norm_float(nutrition.get("sodiumContent"))
        if all(v is None for v in [kcal, protein, carbs, fat, fiber, salt]):
            # Attempt AH-specific scan
            nutrit = _scan_for_nutrition(prod)
            kcal = nutrit.get("kcal")
            protein = nutrit.get("protein_g")
            carbs = nutrit.get("carbs_g")
            fat = nutrit.get("fat_g")
            fiber = nutrit.get("fiber_g")
            salt = nutrit.get("salt_g")
    row = {
        "ah_id": _extract_ah_id_from_url(url),
        "name": name,
        "brand": brand,
        "category": None,
        "unit": None,
        "price_eur": None,
        "kcal_per_100": kcal,
        "protein_g_per_100": protein,
        "carbs_g_per_100": carbs,
        "fat_g_per_100": fat,
        "fiber_g_per_100": fiber,
        "salt_g_per_100": salt,
        "nutrition_json": json.dumps(prod) if prod else None,
        "url": url,
        "image_url": None,
        "last_seen": datetime.utcnow().isoformat(),
    }
    return row


def _extract_name_from_html(html: str) -> Optional[str]:
    # og:title
    m = re.search(r"<meta[^>]+property=\"og:title\"[^>]+content=\"(.*?)\"", html, re.I)
    if m:
        return _clean_html_text(m.group(1))
    # <title>
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        title = _clean_html_text(m.group(1))
        # Trim common suffixes
        title = re.sub(r"\s*[|\-]\s*Albert\s*Heijn.*$", "", title, flags=re.I)
        title = re.sub(r"\s*[|\-]\s*ah\.nl.*$", "", title, flags=re.I)
        return title.strip()
    # <h1>
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
    if m:
        return _clean_html_text(m.group(1))
    return None


def _extract_image_from_html(html: str) -> Optional[str]:
    m = re.search(r"<meta[^>]+property=\"og:image\"[^>]+content=\"(.*?)\"", html, re.I)
    if m:
        return m.group(1)
    return None


def _clean_html_text(s: str) -> str:
    # Strip tags and collapse whitespace
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def discover_product_urls_from_sitemap(sitemap_url: str) -> List[str]:
    status, data = fetch(sitemap_url, delay_s=0.5)
    if status != 200:
        return []
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return []
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls: List[str] = []
    for loc in root.findall(".//sm:loc", ns):
        text = (loc.text or "").strip()
        if not text:
            continue
        if "/producten/product/" in text:
            urls.append(text)
        elif text.endswith(".xml"):
            urls.extend(discover_product_urls_from_sitemap(text))
    # Dedupe while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def crawl_ah_products(conn, seed_sitemaps: List[str], limit: int = 1000, progress=None) -> int:
    count = 0
    for sm in seed_sitemaps:
        for url in discover_product_urls_from_sitemap(sm):
            if count >= limit:
                return count
            if is_seen(conn, url):
                continue
            try:
                row = scrape_ah_product_page(url)
                upsert_product(conn, row)
                # Commit immediately so UI can see new rows during crawl
                try:
                    conn.commit()
                except Exception:
                    pass
                mark_seen_page(conn, url, "ah_product", "ok", 200, None)
                count += 1
                if progress:
                    try:
                        progress({"status": "ok", "url": url})
                    except Exception:
                        pass
            except Exception as e:
                mark_seen_page(conn, url, "ah_product", "error", None, str(e))
                if progress:
                    try:
                        progress({"status": "error", "url": url, "error": str(e)})
                    except Exception:
                        pass
    return count
