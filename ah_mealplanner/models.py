from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Product:
    name: str
    ah_id: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    unit: Optional[str] = None
    price_eur: Optional[float] = None
    kcal_per_100: Optional[float] = None
    protein_g_per_100: Optional[float] = None
    carbs_g_per_100: Optional[float] = None
    fat_g_per_100: Optional[float] = None
    fiber_g_per_100: Optional[float] = None
    salt_g_per_100: Optional[float] = None
    nutrition_json: Optional[str] = None
    url: Optional[str] = None
    image_url: Optional[str] = None
    last_seen: Optional[str] = None


@dataclass
class Ingredient:
    name: str
    quantity: Optional[float] = None
    unit: Optional[str] = None
    product_id: Optional[int] = None
    raw: Optional[str] = None


@dataclass
class Recipe:
    title: str
    source: Optional[str] = None
    source_id: Optional[str] = None
    url: Optional[str] = None
    image_url: Optional[str] = None
    servings: Optional[int] = None
    total_time_min: Optional[int] = None
    kcal_per_serving: Optional[float] = None
    protein_g_per_serving: Optional[float] = None
    carbs_g_per_serving: Optional[float] = None
    fat_g_per_serving: Optional[float] = None
    fiber_g_per_serving: Optional[float] = None
    instructions: Optional[str] = None
    raw_json: Optional[str] = None
    last_seen: Optional[str] = None
    ingredients: Optional[List[Ingredient]] = None

