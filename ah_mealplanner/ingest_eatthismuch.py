"""Ingest EatThisMuch foods and recipes."""

import json
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from .http import fetch
from .db import upsert_product, insert_recipe, insert_recipe_tags

ETM_BASE = "https://www.eatthismuch.com"


def _get_json(path: str, delay_s: float) -> Dict:
    url = urljoin(ETM_BASE, path)
    status, data = fetch(url, delay_s=delay_s)
    if status != 200:
        raise ValueError(f"HTTP {status}: {url}")
    return json.loads(data)


def _food_to_row(obj: Dict) -> Dict:
    img = obj.get("default_image", {}) or {}
    image_url = urljoin(ETM_BASE + "/", img.get("image", "")) if img.get("image") else None
    public_url = obj.get("public_url") or obj.get("canonical_url")
    url = urljoin(ETM_BASE + "/", public_url.lstrip("/")) if public_url else None
    sodium = obj.get("sodium")
    salt_g = sodium / 1000.0 if sodium is not None else None
    row = {
        "ah_id": f"etm_food_{obj.get('id')}",
        "name": obj.get("food_name"),
        "brand": obj.get("manufactured_by"),
        "category": str(obj.get("food_group")) if obj.get("food_group") is not None else None,
        "unit": "g",
        "price_eur": obj.get("price"),
        "kcal_per_100": obj.get("calories"),
        "protein_g_per_100": obj.get("proteins"),
        "carbs_g_per_100": obj.get("carbs"),
        "fat_g_per_100": obj.get("fats"),
        "fiber_g_per_100": obj.get("fiber"),
        "salt_g_per_100": salt_g,
        "nutrition_json": json.dumps(obj.get("nutrition")) if obj.get("nutrition") else None,
        "url": url,
        "image_url": image_url,
        "last_seen": datetime.utcnow().isoformat(),
    }
    return row


def crawl_etm_foods(
    conn,
    limit: Optional[int] = 100,
    delay_s: float = 0.2,
    progress: Optional[Callable[[Dict], None]] = None,
) -> int:
    count = 0
    next_path: Optional[str] = "/api/v1/food/?page=1"
    while next_path and (limit is None or count < limit):
        data = _get_json(next_path, delay_s)
        for obj in data.get("objects", []):
            fid = obj.get("id")
            try:
                row = _food_to_row(obj)
                upsert_product(conn, row)
                if progress:
                    progress({"status": "ok", "id": fid, "url": row.get("url")})
            except Exception as e:
                if progress:
                    progress({"status": "error", "id": fid, "error": str(e)})
            count += 1
            if limit is not None and count >= limit:
                break
        next_path = data.get("meta", {}).get("next")
    return count


def _recipe_from_obj(obj: Dict) -> Tuple[Dict, List[Dict], List[Dict]]:
    img = obj.get("default_image", {}) or {}
    image_url = urljoin(ETM_BASE + "/", img.get("image", "")) if img.get("image") else None
    public_url = obj.get("public_url") or obj.get("canonical_url")
    url = urljoin(ETM_BASE + "/", public_url.lstrip("/")) if public_url else None
    directions = "\n".join(d.get("text", "").strip() for d in obj.get("directions", []) if d.get("text"))
    recipe_row = {
        "source": "eatthismuch",
        "source_id": str(obj.get("id")),
        "title": obj.get("food_name"),
        "url": url,
        "image_url": image_url,
        "servings": obj.get("number_servings"),
        "total_time_min": obj.get("total_time"),
        "kcal_per_serving": obj.get("serving_calories"),
        "protein_g_per_serving": obj.get("serving_proteins"),
        "carbs_g_per_serving": obj.get("serving_carbs"),
        "fat_g_per_serving": obj.get("serving_fats"),
        "fiber_g_per_serving": obj.get("fiber"),
        "instructions": directions,
        "raw_json": json.dumps(obj),
        "last_seen": datetime.utcnow().isoformat(),
    }
    ingredients: List[Dict] = []
    for ing in obj.get("ingredients", []):
        food = ing.get("food", {}) or {}
        ingredients.append(
            {
                "name": food.get("food_name"),
                "quantity": ing.get("amount"),
                "unit": str(ing.get("units")) if ing.get("units") is not None else None,
                "product_id": None,
                "raw": json.dumps(ing),
            }
        )
    tags: List[Dict] = []
    tag_cloud = obj.get("tag_cloud")
    tag_items: List[str] = []
    if isinstance(tag_cloud, list):
        tag_items = tag_cloud
    elif isinstance(tag_cloud, str):
        tag_items = [t.strip().strip('"') for t in tag_cloud.split() if t.strip()]
    for t in tag_items:
        tags.append({"tag": t, "type": None})
    return recipe_row, ingredients, tags


def crawl_etm_recipes(
    conn,
    limit: Optional[int] = 100,
    delay_s: float = 0.2,
    progress: Optional[Callable[[Dict], None]] = None,
) -> int:
    count = 0
    next_path: Optional[str] = "/api/v1/recipe/?page=1"
    while next_path and (limit is None or count < limit):
        data = _get_json(next_path, delay_s)
        for obj in data.get("objects", []):
            rid = obj.get("id")
            try:
                recipe_row, ingredients, tags = _recipe_from_obj(obj)
                db_rid = insert_recipe(conn, recipe_row, ingredients)
                if tags:
                    insert_recipe_tags(conn, db_rid, tags)
                if progress:
                    progress({"status": "ok", "id": rid, "url": recipe_row.get("url")})
            except Exception as e:
                if progress:
                    progress({"status": "error", "id": rid, "error": str(e)})
            count += 1
            if limit is not None and count >= limit:
                break
        next_path = data.get("meta", {}).get("next")
    return count

