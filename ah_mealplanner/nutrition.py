from typing import Dict, Optional

from .ingredient_parser import unit_to_grams, parse_quantity_unit_name


def parse_and_update_ingredients(conn, recipe_id: int) -> int:
    cur = conn.cursor()
    rows = cur.execute("select id, raw, name, quantity, unit from ingredients where recipe_id=?", (recipe_id,)).fetchall()
    updated = 0
    for r in rows:
        raw = r["raw"] or r["name"] or ""
        name, qty, unit = parse_quantity_unit_name(raw)
        if name != r["name"] or qty != r["quantity"] or unit != r["unit"]:
            cur.execute("update ingredients set name=?, quantity=?, unit=? where id=?", (name, qty, unit, r["id"]))
            updated += 1
    return updated


def compute_recipe_nutrition_from_products(conn, recipe_id: int, servings: Optional[int]) -> Dict[str, float]:
    """Compute totals from ingredient product links and quantities.

    Returns dict with totals per serving (kcal, protein_g, carbs_g, fat_g, fiber_g) if possible.
    Missing data is treated as zero; ingredients without grams are skipped.
    """
    cur = conn.cursor()
    ings = cur.execute("select product_id, quantity, unit from ingredients where recipe_id=?", (recipe_id,)).fetchall()
    totals = {"kcal": 0.0, "protein_g": 0.0, "carbs_g": 0.0, "fat_g": 0.0, "fiber_g": 0.0}
    for ing in ings:
        pid = ing["product_id"]
        if not pid:
            continue
        grams = unit_to_grams(ing["quantity"], ing["unit"]) if ing["quantity"] is not None else None
        if grams is None:
            continue
        p = cur.execute("select kcal_per_100, protein_g_per_100, carbs_g_per_100, fat_g_per_100, fiber_g_per_100 from products where id=?", (pid,)).fetchone()
        if not p:
            continue
        factor = grams / 100.0
        totals["kcal"] += (p["kcal_per_100"] or 0.0) * factor
        totals["protein_g"] += (p["protein_g_per_100"] or 0.0) * factor
        totals["carbs_g"] += (p["carbs_g_per_100"] or 0.0) * factor
        totals["fat_g"] += (p["fat_g_per_100"] or 0.0) * factor
        totals["fiber_g"] += (p["fiber_g_per_100"] or 0.0) * factor

    if servings and servings > 0:
        per_serv = {k: round(v / servings, 1) for k, v in totals.items()}
    else:
        per_serv = {k: round(v, 1) for k, v in totals.items()}

    # Update recipe if missing values
    r = cur.execute("select kcal_per_serving, protein_g_per_serving, carbs_g_per_serving, fat_g_per_serving, fiber_g_per_serving from recipes where id=?", (recipe_id,)).fetchone()
    need_update = False
    fields = ["kcal_per_serving", "protein_g_per_serving", "carbs_g_per_serving", "fat_g_per_serving", "fiber_g_per_serving"]
    values = [per_serv["kcal"], per_serv["protein_g"], per_serv["carbs_g"], per_serv["fat_g"], per_serv["fiber_g"]]
    if r and any(v is None for v in r):
        need_update = True
    if need_update:
        cur.execute(
            "update recipes set kcal_per_serving=?, protein_g_per_serving=?, carbs_g_per_serving=?, fat_g_per_serving=?, fiber_g_per_serving=? where id=?",
            (*values, recipe_id),
        )

    return per_serv

