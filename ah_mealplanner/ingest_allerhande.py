import json
import re
import urllib.request
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from .http import fetch
from .db import is_seen, mark_seen_page, insert_recipe, insert_recipe_tags
from .ingest_ah import scrape_ah_product_page
from .db import upsert_product
from .nutrition import parse_and_update_ingredients, compute_recipe_nutrition_from_products
import difflib


JSON_LD_RE = re.compile(r"<script[^>]+type=\"application/ld\+json\"[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)


def _extract_json_ld(html: str) -> List[Dict]:
    blocks = JSON_LD_RE.findall(html)
    out = []
    for b in blocks:
        text = b.strip()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Some pages wrap multiple JSON objects in an array or join them; try to recover
            # Attempt to locate JSON objects inside
            try:
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1:
                    data = json.loads(text[start : end + 1])
                else:
                    continue
            except Exception:
                continue
        if isinstance(data, list):
            out.extend(data)
        else:
            out.append(data)
    return out


def _first_recipe(obj_list: List[Dict]) -> Dict:
    for o in obj_list:
        t = o.get("@type")
        if t == "Recipe" or (isinstance(t, list) and "Recipe" in t):
            return o
        # Sometimes under Graph
        if "@graph" in o and isinstance(o["@graph"], list):
            for g in o["@graph"]:
                gt = g.get("@type")
                if gt == "Recipe" or (isinstance(gt, list) and "Recipe" in gt):
                    return g
    return {}


def _extract_nutrition_from_jsonld(recipe_obj: Dict) -> Dict:
    nutrition = recipe_obj.get("nutrition", {}) or {}
    kcal = _to_float(nutrition.get("calories"))
    protein = _to_float(nutrition.get("proteinContent"))
    carbs = _to_float(nutrition.get("carbohydrateContent"))
    fat = _to_float(nutrition.get("fatContent"))
    fiber = _to_float(nutrition.get("fiberContent"))
    # Alternate keys sometimes used
    if kcal is None:
        kcal = _to_float(nutrition.get("energy")) or _to_float(nutrition.get("energie"))
    if protein is None:
        protein = _to_float(nutrition.get("protein")) or _to_float(nutrition.get("eiwit"))
    if carbs is None:
        carbs = _to_float(nutrition.get("carbohydrates")) or _to_float(nutrition.get("koolhydraten"))
    if fat is None:
        fat = _to_float(nutrition.get("fat")) or _to_float(nutrition.get("vetten"))
    if fiber is None:
        fiber = _to_float(nutrition.get("fiber")) or _to_float(nutrition.get("vezels"))
    return {
        "kcal": kcal,
        "protein": protein,
        "carbs": carbs,
        "fat": fat,
        "fiber": fiber,
    }


NUTR_LABELS = [
    ("kcal", re.compile(r"(\d+[\.,]?\d*)\s*kcal", re.I)),
    ("protein", re.compile(r"(\d+[\.,]?\d*)\s*(?:g\s*)?(?:eiwit|protein|prote[iï]ne)n?", re.I)),
    ("carbs", re.compile(r"(\d+[\.,]?\d*)\s*(?:g\s*)?(?:koolhydraat|carb|carbohydr)en?", re.I)),
    ("fat", re.compile(r"(\d+[\.,]?\d*)\s*(?:g\s*)?(?:vet|fat)ten?", re.I)),
    ("fiber", re.compile(r"(\d+[\.,]?\d*)\s*(?:g\s*)?(?:vezel|fiber)s?", re.I)),
]


def _extract_nutrition_from_html(html: str) -> Dict:
    """Best-effort HTML fallback to find per-serving nutrition values.

    Searches for labels like kcal, eiwit/protein, koolhydraten/carbs, vet/fat, vezels/fiber.
    """
    out: Dict[str, Optional[float]] = {"kcal": None, "protein": None, "carbs": None, "fat": None, "fiber": None}
    # Prefer structured microdata blocks if present
    # Simple regex scan as a fallback
    txt = html
    for key, rx in NUTR_LABELS:
        m = rx.search(txt)
        if m:
            try:
                out[key] = float(m.group(1).replace(",", "."))
            except Exception:
                pass
    return out


def refresh_nutrition(
    conn,
    limit: int = 2000,
    missing_only: bool = True,
    delay_s: float = 0.5,
    progress=None,
):
    """Refresh nutrition fields for recipes by refetching their source pages.

    Emits progress events via callback: {status: 'ok'|'error', url, id, updated: bool}
    """
    cur = conn.cursor()
    # Pick candidates
    if missing_only:
        rows = cur.execute(
            """
            select id, url from recipes
            where url like 'http%'
              and (
                 coalesce(kcal_per_serving,0)=0 or coalesce(protein_g_per_serving,0)=0 or
                 coalesce(carbs_g_per_serving,0)=0 or coalesce(fat_g_per_serving,0)=0 or
                 coalesce(fiber_g_per_serving,0)=0
              )
            order by id desc
            limit ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = cur.execute(
            "select id, url from recipes where url like 'http%' order by id desc limit ?",
            (limit,),
        ).fetchall()
    updated = 0
    for r in rows:
        rid, url = r["id"], r["url"]
        try:
            status, data = fetch(url, delay_s=delay_s)
            if status != 200:
                if progress:
                    try:
                        progress({"status": "error", "id": rid, "url": url, "error": f"http {status}"})
                    except Exception:
                        pass
                continue
            html = data.decode("utf-8", "ignore")
            jld = _extract_json_ld(html)
            obj = _first_recipe(jld) if jld else None
            nut = _extract_nutrition_from_jsonld(obj) if obj else {}
            kcal = nut.get("kcal")
            pr = nut.get("protein")
            cb = nut.get("carbs")
            ft = nut.get("fat")
            fb = nut.get("fiber")
            if any(x is None for x in (kcal, pr, cb, ft, fb)):
                hn = _extract_nutrition_from_html(html)
                kcal = kcal if kcal is not None else hn.get("kcal")
                pr = pr if pr is not None else hn.get("protein")
                cb = cb if cb is not None else hn.get("carbs")
                ft = ft if ft is not None else hn.get("fat")
                fb = fb if fb is not None else hn.get("fiber")
            did = False
            if any(x is not None for x in (kcal, pr, cb, ft, fb)):
                cur.execute(
                    "update recipes set kcal_per_serving=?, protein_g_per_serving=?, carbs_g_per_serving=?, fat_g_per_serving=?, fiber_g_per_serving=? where id=?",
                    (kcal, pr, cb, ft, fb, rid),
                )
                did = True
                updated += 1
            if progress:
                try:
                    progress({"status": "ok", "id": rid, "url": url, "updated": did})
                except Exception:
                    pass
        except Exception as e:
            if progress:
                try:
                    progress({"status": "error", "id": rid, "url": url, "error": str(e)})
                except Exception:
                    pass
    return updated


def fetch_recipe(url: str) -> Tuple[Dict, List[Dict], List[Dict]]:
    """Fetch and parse a recipe from a URL that exposes JSON-LD.

    Returns (recipe_row, ingredients_rows). Designed to work with Allerhande
    and any site publishing schema.org Recipe JSON-LD.
    """
    status, data = fetch(url, delay_s=1.0)
    html = data.decode("utf-8", errors="ignore")
    json_ld = _extract_json_ld(html)
    recipe = _first_recipe(json_ld)
    if not recipe:
        raise ValueError("No Recipe JSON-LD found at URL")

    title = recipe.get("name")
    image = recipe.get("image")
    if isinstance(image, dict):
        image_url = image.get("url")
    elif isinstance(image, list) and image:
        image_url = image[0] if isinstance(image[0], str) else image[0].get("url")
    else:
        image_url = None

    # Nutrition: JSON-LD first, then HTML fallback
    nut = _extract_nutrition_from_jsonld(recipe)
    kcal = nut["kcal"]
    protein = nut["protein"]
    carbs = nut["carbs"]
    fat = nut["fat"]
    fiber = nut["fiber"]
    # If missing, try to derive from the whole page HTML
    if any(v is None for v in (kcal, protein, carbs, fat, fiber)):
        html_nut = _extract_nutrition_from_html(html)
        kcal = kcal if kcal is not None else html_nut.get("kcal")
        protein = protein if protein is not None else html_nut.get("protein")
        carbs = carbs if carbs is not None else html_nut.get("carbs")
        fat = fat if fat is not None else html_nut.get("fat")
        fiber = fiber if fiber is not None else html_nut.get("fiber")

    servings = _to_int(recipe.get("recipeYield"))
    total_time_min = _parse_duration_minutes(recipe.get("totalTime"))
    instructions = _join_instructions(recipe.get("recipeInstructions"))

    # Try to derive Allerhande source_id from URL (e.g., R-R1190247)
    src_id = None
    m_id = re.search(r"/allerhande/recept/([^/]+)", url)
    if m_id:
        src_id = m_id.group(1)

    recipe_row = dict(
        source="allerhande",
        source_id=src_id,
        title=title,
        url=url,
        image_url=image_url,
        servings=servings,
        total_time_min=total_time_min,
        kcal_per_serving=kcal,
        protein_g_per_serving=protein,
        carbs_g_per_serving=carbs,
        fat_g_per_serving=fat,
        fiber_g_per_serving=fiber,
        instructions=instructions,
        raw_json=json.dumps(recipe),
        last_seen=datetime.utcnow().isoformat(),
    )

    ingredients = []
    for raw in recipe.get("recipeIngredient", []) or []:
        ingredients.append(dict(name=_clean_ingredient_name(raw), quantity=None, unit=None, product_id=None, raw=raw))

    # Extract tags/themes
    tags: List[Dict] = []
    def add_list(vals, t):
        if not vals:
            return
        if isinstance(vals, str):
            # Split on commas/semicolons
            parts = [v.strip() for v in re.split(r",|;", vals) if v.strip()]
        elif isinstance(vals, list):
            parts = []
            for v in vals:
                if isinstance(v, str):
                    parts.append(v.strip())
                elif isinstance(v, dict) and v.get("name"):
                    parts.append(str(v.get("name")).strip())
        else:
            parts = []
        for v in parts:
            if v:
                tags.append({"tag": v, "type": t})

    add_list(recipe.get("recipeCategory"), "category")
    add_list(recipe.get("recipeCuisine"), "cuisine")
    add_list(recipe.get("keywords"), "keyword")
    add_list(recipe.get("about"), "about")
    add_list(recipe.get("suitableForDiet"), "diet")

    return recipe_row, ingredients, tags


PRODUCT_URL_ABS_RE = re.compile(r"https?://(?:www\.)?ah\.nl/producten/product/[^\"'<>\s]+", re.I)
PRODUCT_URL_REL_HREF_RE = re.compile(r"href\s*=\s*[\"'](/producten/product/[^\"'>\s]+)[\"']", re.I)
PRODUCT_URL_ANY_RE = re.compile(r"(/producten/product/[^\"'<>\s]+)", re.I)
# Handle JSON-escaped slashes in embedded data
PRODUCT_URL_ESC_ABS_RE = re.compile(r"https?:\\/\\/(?:www\\.)?ah\.nl\\/producten\\/product\\/[^\"'<>\s]+", re.I)
PRODUCT_URL_ESC_REL_RE = re.compile(r"\\/producten\\/product\\/[^\"'<>\s]+", re.I)
# Product IDs often look like 'wiXXXXXXXX'; capture to synthesize URLs if needed
PRODUCT_ID_RE = re.compile(r"\bwi[\w-]{6,}\b", re.I)


def _to_abs_ah(u: str) -> str:
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("/"):
        return "https://www.ah.nl" + u
    return u


def extract_product_urls(html: str) -> List[str]:
    # Collect absolute, relative (href=) and any occurrences of the product path
    urls: List[str] = []
    urls.extend(PRODUCT_URL_ABS_RE.findall(html))
    urls.extend([m.group(1) for m in PRODUCT_URL_REL_HREF_RE.finditer(html)])
    urls.extend(PRODUCT_URL_ANY_RE.findall(html))
    # Also search for JSON-escaped occurrences and unescape
    esc = []
    esc.extend(PRODUCT_URL_ESC_ABS_RE.findall(html))
    esc.extend(PRODUCT_URL_ESC_REL_RE.findall(html))
    for e in esc:
        urls.append(e.replace("\\/", "/"))
    # If still nothing obvious, try to synthesize from product IDs embedded in data
    # This is a heuristic fallback.
    if not urls:
        ids = PRODUCT_ID_RE.findall(html)
        for pid in ids:
            urls.append(f"/producten/product/{pid}")
    # Normalize to absolute and dedupe while preserving order
    seen = set()
    out: List[str] = []
    for u in urls:
        au = _to_abs_ah(u)
        if au not in seen:
            seen.add(au)
            out.append(au)
    return out


def verify_recipe_products(conn, recipe_id: Optional[int] = None, url: Optional[str] = None) -> Dict:
    """Fetch a recipe page, extract product URLs, and compare against DB entries.

    Returns dict with keys:
      - url: recipe URL fetched
      - discovered_urls: list of product URLs found on page
      - present: list of dicts {url, id, name}
      - missing: list of URLs not present in DB
      - linked_products: list of dicts {ingredient_id, name, product_id, product_name, product_url}
    """
    cur = conn.cursor()
    if not url and recipe_id is not None:
        r = cur.execute("select url from recipes where id=?", (recipe_id,)).fetchone()
        if not r or not r["url"]:
            raise ValueError("Recipe URL not found in DB")
        url = r["url"]
    if not url:
        raise ValueError("Recipe URL is required")
    status, data = fetch(url, delay_s=0.5)
    if status != 200:
        raise RuntimeError(f"Failed to fetch recipe page: HTTP {status}")
    html = data.decode("utf-8", errors="ignore")
    urls = extract_product_urls(html)
    # Present in DB
    present = []
    missing = []
    if urls:
        qmarks = ",".join(["?"] * len(urls))
        rows = cur.execute(f"select id, name, url from products where url in ({qmarks})", urls).fetchall()
        present_map = {row["url"]: row for row in rows}
        for u in urls:
            if u in present_map:
                row = present_map[u]
                present.append({"url": u, "id": row["id"], "name": row["name"]})
            else:
                missing.append(u)
    # Linked ingredients -> products
    linked = []
    if recipe_id is None:
        r = cur.execute("select id from recipes where url=?", (url,)).fetchone()
        if r:
            recipe_id = r["id"]
    if recipe_id is not None:
        rows = cur.execute(
            """
            select i.id as ingredient_id, i.name as ingredient_name, p.id as product_id, p.name as product_name, p.url as product_url
            from ingredients i left join products p on i.product_id = p.id
            where i.recipe_id=? and i.product_id is not null
            """,
            (recipe_id,),
        ).fetchall()
        for r in rows:
            linked.append({
                "ingredient_id": r["ingredient_id"],
                "name": r["ingredient_name"],
                "product_id": r["product_id"],
                "product_name": r["product_name"],
                "product_url": r["product_url"],
            })
    return {
        "url": url,
        "discovered_urls": urls,
        "present": present,
        "missing": missing,
        "linked_products": linked,
    }


def _norm(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def link_recipe_products(
    conn,
    recipe_id: int,
    recipe_url: str,
    max_products: int = 50,
    match_threshold: float = 0.62,
    progress: Optional[Callable[[Dict], None]] = None,
) -> int:
    """Fetch the recipe page, scrape linked AH product pages, upsert products, and
    try to map recipe ingredients to these products using fuzzy matching.

    Returns number of ingredients linked.
    """
    # Fetch HTML
    status, data = fetch(recipe_url, delay_s=0.5)
    if status != 200:
        if progress:
            try:
                progress({"status": "error", "url": recipe_url, "error": f"http {status} fetching recipe"})
            except Exception:
                pass
        return 0
    html = data.decode("utf-8", errors="ignore")
    product_urls = extract_product_urls(html)[:max_products]
    if progress:
        try:
            progress({"status": "info", "url": recipe_url, "found": len(product_urls)})
        except Exception:
            pass

    # Upsert products
    prod_rows = []
    for pu in product_urls:
        try:
            row = scrape_ah_product_page(pu)
            pid = upsert_product(conn, row)
            prod_rows.append((pid, row.get("name") or ""))
            if progress:
                try:
                    progress({"status": "ok", "url": pu, "name": row.get("name")})
                except Exception:
                    pass
        except Exception:
            if progress:
                try:
                    progress({"status": "error", "url": pu, "error": "scrape failed"})
                except Exception:
                    pass
            continue

    if not prod_rows:
        return 0

    # Attempt to match ingredients
    cur = conn.cursor()
    ings = cur.execute("select id, name, raw from ingredients where recipe_id=?", (recipe_id,)).fetchall()
    linked = 0
    for ing in ings:
        text = (ing["raw"] or ing["name"] or "").strip()
        if not text:
            continue
        nt = _norm(text)
        # score against product names (combined token overlap + sequence ratio)
        best = None
        best_score = 0.0
        for pid, pname in prod_rows:
            pn = _norm(pname)
            tok_nt = set(nt.split())
            tok_pn = set(pn.split())
            overlap = 0.0
            if tok_nt and tok_pn:
                overlap = len(tok_nt & tok_pn) / len(tok_nt | tok_pn)
            seq = difflib.SequenceMatcher(a=nt, b=pn).ratio()
            score = 0.6 * seq + 0.4 * overlap
            if score > best_score:
                best_score = score
                best = pid
        if best is not None and best_score >= match_threshold:
            cur.execute("update ingredients set product_id=? where id=?", (best, ing["id"]))
            linked += 1
    return linked


def enrich_recipe(
    conn,
    recipe_id: int,
    recipe_url: str,
    servings: Optional[int],
    product_progress: Optional[Callable[[Dict], None]] = None,
    *,
    link_products: bool = False,
    compute_nutrition: bool = False,
) -> Dict:
    """Run post-processing: parse ingredient quantities, link products, and compute nutrition.

    Returns a dict with counts and computed nutrition.
    """
    out = {"parsed": 0, "linked": 0, "computed": None}
    try:
        out["parsed"] = parse_and_update_ingredients(conn, recipe_id)
    except Exception:
        pass
    if link_products:
        try:
            out["linked"] = link_recipe_products(conn, recipe_id, recipe_url, progress=product_progress)
        except Exception:
            pass
    if compute_nutrition:
        try:
            per_serv = compute_recipe_nutrition_from_products(conn, recipe_id, servings)
            out["computed"] = per_serv
        except Exception:
            pass
    return out


def discover_recipe_urls_from_sitemap(sitemap_url: str) -> List[str]:
    status, data = fetch(sitemap_url, delay_s=0.5)
    if status != 200:
        return []
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return []
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    # Handle both index and urlset
    for loc in root.findall(".//sm:loc", ns):
        text = (loc.text or "").strip()
        if not text:
            continue
        if "/allerhande/recept/" in text:
            urls.append(text)
        elif text.endswith(".xml") and "allerhande" in text:
            # Nested sitemap, fetch it
            urls.extend(discover_recipe_urls_from_sitemap(text))
    return list(dict.fromkeys(urls))


def crawl_allerhande(
    conn,
    seed_sitemaps: List[str],
    limit: int = 1000,
    delay_s: float = 1.0,
    progress=None,
    product_progress: Optional[Callable[[Dict], None]] = None,
) -> int:
    count = 0
    seen = set()
    for sm in seed_sitemaps:
        for u in discover_recipe_urls_from_sitemap(sm):
            if count >= limit:
                return count
            if u in seen:
                continue
            seen.add(u)
            if is_seen(conn, u):
                continue
            try:
                r, ings, tags = fetch_recipe(u)
                rid = insert_recipe(conn, r, ings)
                try:
                    insert_recipe_tags(conn, rid, tags)
                except Exception:
                    pass
                # Parse quantities only; skip product scraping and nutrition computation
                try:
                    enrich_recipe(conn, rid, u, r.get('servings'), None, link_products=False, compute_nutrition=False)
                except Exception:
                    pass
                # Commit immediately so UI can see new rows during crawl
                try:
                    conn.commit()
                except Exception:
                    pass
                mark_seen_page(conn, u, "allerhande_recipe", "ok", 200, None)
                count += 1
                if progress:
                    try:
                        progress({"status": "ok", "url": u})
                    except Exception:
                        pass
            except Exception as e:
                mark_seen_page(conn, u, "allerhande_recipe", "error", None, str(e))
                if progress:
                    try:
                        progress({"status": "error", "url": u, "error": str(e)})
                    except Exception:
                        pass
    return count


def _to_float(value) -> float:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", s)
    return float(m.group(1)) if m else None


def _to_int(value) -> int:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        m = re.search(r"([0-9]+)", value)
        return int(m.group(1)) if m else None
    return None


def _parse_duration_minutes(iso8601: str) -> int:
    if not iso8601:
        return None
    # Robust ISO 8601 duration parser for PT#H#M#S (ignore days/years)
    # Extract number-letter pairs
    pairs = re.findall(r"(\d+)([HMS])", iso8601)
    if not pairs:
        return None
    hours = minutes = seconds = 0
    for num, unit in pairs:
        n = int(num)
        if unit == 'H':
            hours = n
        elif unit == 'M':
            minutes = n
        elif unit == 'S':
            seconds = n
    total = hours * 60 + minutes + (1 if seconds >= 30 else 0)
    return total


def _join_instructions(instr) -> str:
    if instr is None:
        return None
    if isinstance(instr, str):
        return instr
    if isinstance(instr, list):
        parts = []
        for step in instr:
            if isinstance(step, str):
                parts.append(step)
            elif isinstance(step, dict) and "text" in step:
                parts.append(step["text"])
        return "\n".join(parts)
    return None


def _clean_ingredient_name(raw: str) -> str:
    # Keep it simple; you can improve normalization later
    return re.sub(r"\s+", " ", raw).strip()
