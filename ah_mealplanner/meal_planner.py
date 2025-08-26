import math
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import sqlite3


@dataclass
class PlanItem:
    item_type: str  # 'recipe' or 'product'
    item_id: int
    servings: float
    calories: float
    protein_g: float
    carbs_g: float
    fat_g: float
    title: str


def _calc_recipe_macros(row: sqlite3.Row, servings: float = 1.0) -> Tuple[float, float, float, float]:
    kcal = (row["kcal_per_serving"] or 0.0) * servings
    p = (row["protein_g_per_serving"] or 0.0) * servings
    c = (row["carbs_g_per_serving"] or 0.0) * servings
    f = (row["fat_g_per_serving"] or 0.0) * servings
    return kcal, p, c, f


def _calc_product_macros(row: sqlite3.Row, grams: float) -> Tuple[float, float, float, float]:
    factor = grams / 100.0
    kcal = (row["kcal_per_100"] or 0.0) * factor
    p = (row["protein_g_per_100"] or 0.0) * factor
    c = (row["carbs_g_per_100"] or 0.0) * factor
    f = (row["fat_g_per_100"] or 0.0) * factor
    return kcal, p, c, f


def _totals(items: List[PlanItem]) -> Dict[str, float]:
    kcal = sum(i.calories for i in items)
    p = sum(i.protein_g for i in items)
    c = sum(i.carbs_g for i in items)
    f = sum(i.fat_g for i in items)
    return {"calories": round(kcal, 1), "protein_g": round(p, 1), "carbs_g": round(c, 1), "fat_g": round(f, 1)}


def _filter_by_exclusions(name: str, exclusions: List[str]) -> bool:
    n = name.lower()
    return any(ex.lower() in n for ex in exclusions)


def generate_daily_plan(
    conn: sqlite3.Connection,
    target_calories: float = 2200,
    meals_per_day: int = 3,
    macro_split: Tuple[float, float, float] = (0.3, 0.35, 0.35),  # protein, fat, carbs (fraction of calories)
    macro_targets: Optional[Dict[str, Optional[float]]] = None,  # grams per day for P/C/F
    exclusions: Optional[List[str]] = None,
    preferred_tags_per_meal: Optional[List[List[str]]] = None,
    date: Optional[str] = None,
    slot_names: Optional[List[str]] = None,
):
    exclusions = exclusions or []
    # Derive default macro targets if not provided or fill in missing ones
    if not macro_targets or all(v is None for v in macro_targets.values()):
        try:
            p_ratio, f_ratio, c_ratio = macro_split
            macro_targets = {
                "protein_g": target_calories * p_ratio / 4.0,
                "fat_g": target_calories * f_ratio / 9.0,
                "carbs_g": target_calories * c_ratio / 4.0,
            }
        except Exception:
            macro_targets = None
    else:
        try:
            p_ratio, f_ratio, c_ratio = macro_split
            macro_targets = {
                "protein_g": macro_targets.get("protein_g") if macro_targets.get("protein_g") is not None else target_calories * p_ratio / 4.0,
                "fat_g": macro_targets.get("fat_g") if macro_targets.get("fat_g") is not None else target_calories * f_ratio / 9.0,
                "carbs_g": macro_targets.get("carbs_g") if macro_targets.get("carbs_g") is not None else target_calories * c_ratio / 4.0,
            }
        except Exception:
            pass
    cur = conn.cursor()
    recipes = cur.execute("SELECT * FROM recipes").fetchall()
    products = cur.execute("SELECT * FROM products").fetchall()

    # Filter by exclusions
    recipes = [r for r in recipes if not _filter_by_exclusions(r["title"] or "", exclusions)]
    products = [p for p in products if not _filter_by_exclusions(p["name"] or "", exclusions)]

    # Basic selection: pick meals_per_day recipes closest to per-meal target calories
    per_meal_target = target_calories / max(1, meals_per_day)
    per_meal_macro_targets: Optional[Dict[str, float]] = None
    if macro_targets:
        try:
            per_meal_macro_targets = {
                "protein_g": float(macro_targets.get("protein_g")) / max(1, meals_per_day) if macro_targets.get("protein_g") is not None else None,
                "carbs_g": float(macro_targets.get("carbs_g")) / max(1, meals_per_day) if macro_targets.get("carbs_g") is not None else None,
                "fat_g": float(macro_targets.get("fat_g")) / max(1, meals_per_day) if macro_targets.get("fat_g") is not None else None,
            }
        except Exception:
            per_meal_macro_targets = None

    def score_recipe(r):
        kcal = r["kcal_per_serving"] or 0.0
        # Start with calorie deviation
        score = abs(per_meal_target - kcal) if kcal else float("inf")
        # If macro targets provided, add deviations for P/C/F where available
        if per_meal_macro_targets:
            try:
                p_t = per_meal_macro_targets.get("protein_g")
                c_t = per_meal_macro_targets.get("carbs_g")
                f_t = per_meal_macro_targets.get("fat_g")
                if p_t is not None:
                    p = r["protein_g_per_serving"] or 0.0
                    score += abs(p_t - p) * 2.0  # prioritize protein slightly
                if c_t is not None:
                    c = r["carbs_g_per_serving"] or 0.0
                    score += abs(c_t - c) * 1.0
                if f_t is not None:
                    f = r["fat_g_per_serving"] or 0.0
                    score += abs(f_t - f) * 1.2
            except Exception:
                pass
        return score

    chosen = []
    used_ids = set()
    for meal_idx in range(meals_per_day):
        candidates = recipes
        # If preferred tags provided for this meal, try to select from tagged recipes first
        if preferred_tags_per_meal and meal_idx < len(preferred_tags_per_meal):
            tags = [t.lower() for t in preferred_tags_per_meal[meal_idx] if t]
            if tags:
                qmarks = ",".join(["?"] * len(tags))
                tagged = cur.execute(
                    f"""
                    SELECT DISTINCT r.*
                    FROM recipes r
                    JOIN recipe_tags t ON r.id = t.recipe_id
                    WHERE lower(t.tag) IN ({qmarks})
                    """,
                    tags,
                ).fetchall()
                # Filter exclusions and already used
                tagged = [r for r in tagged if not _filter_by_exclusions(r["title"] or "", exclusions) and r["id"] not in used_ids]
                if tagged:
                    candidates = tagged
        # Fallback to all recipes (minus already used)
        if candidates is recipes:
            candidates = [r for r in recipes if r["id"] not in used_ids]
        # Choose closest to per-meal target
        candidates_sorted = sorted(candidates, key=score_recipe)
        if candidates_sorted:
            picked = candidates_sorted[0]
            chosen.append(picked)
            used_ids.add(picked["id"])

    items: List[PlanItem] = []
    for r in chosen:
        kcal, p, c, f = _calc_recipe_macros(r, 1.0)
        items.append(PlanItem("recipe", r["id"], 1.0, kcal, p, c, f, r["title"]))

    totals = _totals(items)
    # Adjust with up to two product snacks to reduce gaps
    if products:
        # If we have macro targets, try to pick products that reduce macro error
        def macro_error(totals_now: Dict[str, float]) -> float:
            if not macro_targets:
                return 0.0
            err = 0.0
            for key, weight in (("protein_g", 2.0), ("carbs_g", 1.0), ("fat_g", 1.2)):
                tgt = macro_targets.get(key)
                if tgt is None:
                    continue
                cur = totals_now.get(key, 0.0)
                err += weight * abs((tgt or 0.0) - cur)
            # include calorie deviation lightly
            err += 0.5 * abs(target_calories - totals_now.get("calories", 0.0))
            return err

        for _ in range(2):
            gap_cal = target_calories - totals["calories"]
            current_err = macro_error(totals) if macro_targets else abs(target_calories - totals["calories"])
            best_choice = None
            best_err = current_err
            for p_row in products:
                kcal100 = p_row["kcal_per_100"] or 0.0
                if kcal100 <= 0:
                    continue
                # Heuristic grams: try to close macro deficits or calorie gap
                grams = 100.0
                if macro_targets:
                    grams = 0.0
                    for key, col in (("protein_g", "protein_g_per_100"), ("carbs_g", "carbs_g_per_100"), ("fat_g", "fat_g_per_100")):
                        tgt = macro_targets.get(key)
                        if tgt is None:
                            continue
                        deficit = tgt - totals.get(key, 0.0)
                        per100 = p_row[col] or 0.0
                        if deficit > 0 and per100 > 0:
                            grams = max(grams, deficit / per100 * 100.0)
                    if grams <= 0:
                        grams = 50.0
                    grams = min(300.0, max(25.0, grams))
                else:
                    if gap_cal > target_calories * 0.05:
                        grams = min(200.0, max(50.0, (gap_cal / kcal100) * 100.0))
                kcal, p, c, f = _calc_product_macros(p_row, grams)
                test_totals = {
                    "calories": totals["calories"] + kcal,
                    "protein_g": totals["protein_g"] + p,
                    "carbs_g": totals["carbs_g"] + c,
                    "fat_g": totals["fat_g"] + f,
                }
                err = macro_error(test_totals) if macro_targets else abs(target_calories - test_totals["calories"])
                if err < best_err:
                    best_err = err
                    best_choice = (p_row, grams, kcal, p, c, f)
            if best_choice and best_err < current_err:
                p_row, grams, kcal, p, c, f = best_choice
                items.append(PlanItem("product", p_row["id"], grams / 100.0, kcal, p, c, f, p_row["name"]))
                totals = _totals(items)
            else:
                break

    # Save plan
    from .db import save_meal_plan
    plan_id = save_meal_plan(
        conn,
        date or "today",
        target_calories,
        meals_per_day,
        totals,
        items=[
            dict(item_type=i.item_type, item_id=i.item_id, servings=i.servings, notes=i.title)
            for i in items
        ],
        macro_targets=macro_targets,
        slots=slot_names,
    )
    return plan_id, items, totals


def generate_weekly_plan(
    conn: sqlite3.Connection,
    start_date: str,
    days: int = 7,
    target_calories: float = 2200,
    meals_per_day: int = 3,
    macro_targets: Optional[Dict[str, Optional[float]]] = None,
    exclusions: Optional[List[str]] = None,
    preferred_tags_per_meal: Optional[List[List[str]]] = None,
):
    """Generate plans for a consecutive range of days, saving each.

    Returns list of (date, plan_id, totals)
    """
    from datetime import datetime, timedelta
    exclusions = exclusions or []
    d0 = datetime.fromisoformat(start_date)
    out = []
    for i in range(days):
        day = (d0 + timedelta(days=i)).date().isoformat()
        plan_id, items, totals = generate_daily_plan(
            conn,
            target_calories=target_calories,
            meals_per_day=meals_per_day,
            macro_targets=macro_targets,
            exclusions=exclusions,
            preferred_tags_per_meal=preferred_tags_per_meal,
            date=day,
        )
        out.append((day, plan_id, totals))
    return out
