<<<<<<< HEAD
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
    directions = "n".join(d.get("text", "").strip() for d in obj.get("directions", []) if d.get("text"))
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
    tag_cloud = obj.get("tag_cloud") or []
    if isinstance(tag_cloud, list):
        for t in tag_cloud:
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
=======
 """Ingest EatThisMuch foods and recipes."""
    2 
    3 import json
    4 from datetime import datetime
    5 from typing import Callable, Dict, List, Optional, Tuple
    6 from urllib.parse import urljoin
    7 
    8 from .http import fetch
    9 from .db import upsert_product, insert_recipe, insert_recipe_tags
   10 
   11 ETM_BASE = "https://www.eatthismuch.com"
   12 
   13
   14 def _get_json(path: str, delay_s: float) -> Dict:
   15     url = urljoin(ETM_BASE, path)
   16     status, data = fetch(url, delay_s=delay_s)
   17     if status != 200:
   18         raise ValueError(f"HTTP {status}: {url}")
   19     return json.loads(data)
   20
   21
   22 def _food_to_row(obj: Dict) -> Dict:
   23     img = obj.get("default_image", {}) or {}
   24     image_url = urljoin(ETM_BASE + "/", img.get("image", "")) if img.get("image") else None
   25     public_url = obj.get("public_url") or obj.get("canonical_url")
   26     url = urljoin(ETM_BASE + "/", public_url.lstrip("/")) if public_url else None
   27     sodium = obj.get("sodium")
   28     salt_g = sodium / 1000.0 if sodium is not None else None
   29     row = {
   30         "ah_id": f"etm_food_{obj.get('id')}",
   31         "name": obj.get("food_name"),
   32         "brand": obj.get("manufactured_by"),
   33         "category": str(obj.get("food_group")) if obj.get("food_group") is not None else None,
   34         "unit": "g",
   35         "price_eur": obj.get("price"),
   36         "kcal_per_100": obj.get("calories"),
   37         "protein_g_per_100": obj.get("proteins"),
   38         "carbs_g_per_100": obj.get("carbs"),
   39         "fat_g_per_100": obj.get("fats"),
   40         "fiber_g_per_100": obj.get("fiber"),
   41         "salt_g_per_100": salt_g,
   42         "nutrition_json": json.dumps(obj.get("nutrition")) if obj.get("nutrition") else None,
   43         "url": url,
   44         "image_url": image_url,
   45         "last_seen": datetime.utcnow().isoformat(),
   46     }
   47     return row
   48
   


def crawl_etm_foods(
    51     conn,
    52     limit: Optional[int] = 100,
    53     delay_s: float = 0.2,
    54     progress: Optional[Callable[[Dict], None]] = None,
    55 ) -> int:
    56     count = 0
    57     next_path: Optional[str] = "/api/v1/food/?page=1"
    58     while next_path and (limit is None or count < limit):
    59         data = _get_json(next_path, delay_s)
    60         for obj in data.get("objects", []):
    61             fid = obj.get("id")
    62             try:
    63                 row = _food_to_row(obj)
    64                 upsert_product(conn, row)
    65                 if progress:
    66                     progress({"status": "ok", "id": fid, "url": row.get("url")})
    67             except Exception as e:
    68                 if progress:
    69                     progress({"status": "error", "id": fid, "error": str(e)})
    70             count += 1
    71             if limit is not None and count >= limit:
    72                 break
    73         next_path = data.get("meta", {}).get("next")
    74     return count
    75
    76
    77 def _recipe_from_obj(obj: Dict) -> Tuple[Dict, List[Dict], List[Dict]]:
    78     img = obj.get("default_image", {}) or {}
    79     image_url = urljoin(ETM_BASE + "/", img.get("image", "")) if img.get("image") else None
    80     public_url = obj.get("public_url") or obj.get("canonical_url")
    81     url = urljoin(ETM_BASE + "/", public_url.lstrip("/")) if public_url else None
    82     directions = "n".join(d.get("text", "").strip() for d in obj.get("directions", []) if d.get("text"))
    83     recipe_row = {
    84         "source": "eatthismuch",
    85         "source_id": str(obj.get("id")),
    86         "title": obj.get("food_name"),
    87         "url": url,
    88         "image_url": image_url,
    89         "servings": obj.get("number_servings"),
    90         "total_time_min": obj.get("total_time"),
    91         "kcal_per_serving": obj.get("serving_calories"),
    92         "protein_g_per_serving": obj.get("serving_proteins"),
    93         "carbs_g_per_serving": obj.get("serving_carbs"),
    94         "fat_g_per_serving": obj.get("serving_fats"),
    95         "fiber_g_per_serving": obj.get("fiber"),
    96         "instructions": directions,
    97         "raw_json": json.dumps(obj),
    98         "last_seen": datetime.utcnow().isoformat(),
    99     }
   100     ingredients: List[Dict] = []
   101     for ing in obj.get("ingredients", []):
   102         food = ing.get("food", {}) or {}
   103         ingredients.append(
   104             {
   105                 "name": food.get("food_name"),
   106                 "quantity": ing.get("amount"),
   107                 "unit": str(ing.get("units")) if ing.get("units") is not None else None,
   108                 "product_id": None,
   109                 "raw": json.dumps(ing),
   110             }
   111         )
   112     tags: List[Dict] = []
   113     tag_cloud = obj.get("tag_cloud") or []
   114     if isinstance(tag_cloud, list):
   115         for t in tag_cloud:
   116             tags.append({"tag": t, "type": None})
   117     return recipe_row, ingredients, tags
   118
   119
   120 def crawl_etm_recipes(
   121     conn,
   122     limit: Optional[int] = 100,
   123     delay_s: float = 0.2,
   124     progress: Optional[Callable[[Dict], None]] = None,
   125 ) -> int:
   126     count = 0
   127     next_path: Optional[str] = "/api/v1/recipe/?page=1"
   128     while next_path and (limit is None or count < limit):
   129         data = _get_json(next_path, delay_s)
   130         for obj in data.get("objects", []):
   131             rid = obj.get("id")
   132             try:
   133                 recipe_row, ingredients, tags = _recipe_from_obj(obj)
   134                 db_rid = insert_recipe(conn, recipe_row, ingredients)
   135                 if tags:
   136                     insert_recipe_tags(conn, db_rid, tags)
   137                 if progress:
   138                     progress({"status": "ok", "id": rid, "url": recipe_row.get("url")})
   139             except Exception as e:
   140                 if progress:
   141                     progress({"status": "error", "id": rid, "error": str(e)})
   142             count += 1
   143             if limit is not None and count >= limit:
   144                 break
   145         next_path = data.get("meta", {}).get("next")
   146     return count

>>>>>>> 8319a5f5bc9adc976b5686c568d68662742cae27
