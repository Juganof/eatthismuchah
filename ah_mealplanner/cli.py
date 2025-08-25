import argparse
import json
import os
from datetime import date
from typing import List

from . import db
from .ingest_allerhande import fetch_recipe, crawl_allerhande, enrich_recipe
from .ingest_ah import import_products_from_csv, import_products_from_json, crawl_ah_products
from .ingest_eatthismuch import crawl_etm_foods, crawl_etm_recipes
from .http import fetch
from .ingest_allerhande import _extract_json_ld, _first_recipe, _extract_nutrition_from_jsonld, _extract_nutrition_from_html
from .meal_planner import generate_daily_plan, generate_weekly_plan
# product verification removed


def cmd_init_db(args):
    with db.connect() as conn:
        db.init_db(conn)
    print(f"Initialized DB at {db.DB_PATH}")



def cmd_import_products(args):
    path = args.file
    if path.lower().endswith(".csv"):
        rows = import_products_from_csv(path)
    else:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        rows = import_products_from_json(data)
    with db.connect() as conn:
        db.init_db(conn)
        for r in rows:
            db.upsert_product(conn, r)
    print(f"Imported/updated {len(rows)} products from {path}")


def cmd_ingest_allerhande(args):
    url = args.url
    recipe_row, ingredients, tags = fetch_recipe(url)
    with db.connect() as conn:
        db.init_db(conn)
        rid = db.insert_recipe(conn, recipe_row, ingredients)
        try:
            db.insert_recipe_tags(conn, rid, tags)
        except Exception:
            pass
        try:
            res = enrich_recipe(conn, rid, url, recipe_row.get('servings'), None, link_products=False, compute_nutrition=False)
            print(f"Parsed {res.get('parsed',0)} ingredients")
        except Exception as e:
            print(f"Enrichment failed: {e}")
    print(f"Inserted recipe #{rid}: {recipe_row.get('title')}")


def cmd_plan_day(args):
    target = float(args.calories)
    meals = int(args.meals)
    exclusions: List[str] = []
    if args.exclude:
        exclusions = [x.strip() for x in args.exclude.split(",") if x.strip()]
    preferred = None
    if args.bias_tags:
        preferred = default_meal_tags(meals)
    plan_date = args.date
    if plan_date == "today":
        plan_date = date.today().isoformat()
    with db.connect() as conn:
        db.init_db(conn)
        plan_id, items, totals = generate_daily_plan(
            conn, target, meals, exclusions=exclusions, preferred_tags_per_meal=preferred, date=plan_date
        )
    print(f"Saved plan #{plan_id} for {plan_date}")
    print("Totals:", totals)
    print("Items:")
    for i, it in enumerate(items, 1):
        unit = "servings" if it.item_type == "recipe" else "x100g"
        print(f"  {i}. [{it.item_type}] {it.title} - {it.servings:.2f} {unit} - {it.calories:.0f} kcal")


def cmd_plan_week(args):
    calories = float(args.calories)
    meals = int(args.meals)
    start = args.start
    if start == "today":
        start = date.today().isoformat()
    days = int(args.days)
    macro_targets = {
        "protein_g": args.protein,
        "carbs_g": args.carbs,
        "fat_g": args.fat,
    }
    if all(v is None for v in macro_targets.values()):
        macro_targets = None
    exclusions: List[str] = []
    if getattr(args, "exclude", None):
        exclusions = [x.strip() for x in args.exclude.split(",") if x.strip()]
    preferred = None
    if getattr(args, "bias_tags", False):
        preferred = default_meal_tags(meals)
    with db.connect() as conn:
        db.init_db(conn)
        plans = generate_weekly_plan(
            conn,
            start,
            days=days,
            target_calories=calories,
            meals_per_day=meals,
            macro_targets=macro_targets,
            exclusions=exclusions,
            preferred_tags_per_meal=preferred,
        )
    for day, plan_id, totals in plans:
        print(f"Saved plan #{plan_id} for {day}")
        print("Totals:", totals)


def cmd_crawl_allerhande(args):
    # Known likely sitemap entries; you can override with --sitemap
    seeds = args.sitemap or [
        "https://www.ah.nl/allerhande/sitemap.xml",
        "https://www.ah.nl/sitemaps/sitemap-index.xml",
    ]
    with db.connect() as conn:
        db.init_db(conn)
        ingested = crawl_allerhande(conn, seeds, limit=args.limit, delay_s=1.0)
    print(f"Ingested {ingested} recipes from Allerhande sitemaps")


def cmd_crawl_ah_products(args):
    seeds = args.sitemap or [
        "https://www.ah.nl/sitemaps/sitemap-index.xml",
    ]
    with db.connect() as conn:
        db.init_db(conn)
        ingested = crawl_ah_products(conn, seeds, limit=args.limit)
    print(f"Ingested {ingested} AH products from sitemaps")


def cmd_crawl_etm_foods(args):
    with db.connect() as conn:
        db.init_db(conn)
        ingested = crawl_etm_foods(conn, limit=args.limit, delay_s=args.delay)
    print(f"Ingested {ingested} EatThisMuch foods")


def cmd_crawl_etm_recipes(args):
    with db.connect() as conn:
        db.init_db(conn)
        ingested = crawl_etm_recipes(conn, limit=args.limit, delay_s=args.delay)
    print(f"Ingested {ingested} EatThisMuch recipes")


def build_parser():
    p = argparse.ArgumentParser(description="AH Personal Meal Planner")
    sub = p.add_subparsers(dest="cmd")

    s = sub.add_parser("init-db", help="initialize the SQLite database")
    s.set_defaults(func=cmd_init_db)

    # no sample-data importer: user must provide their own exports

    s = sub.add_parser("import-products", help="import products from JSON/CSV")
    s.add_argument("--file", required=True, help="path to JSON or CSV file")
    s.set_defaults(func=cmd_import_products)

    s = sub.add_parser("ingest-allerhande", help="ingest a recipe from an Allerhande URL")
    s.add_argument("--url", required=True, help="recipe URL")
    s.set_defaults(func=cmd_ingest_allerhande)

    s = sub.add_parser("plan-day", help="generate and save a daily meal plan")
    s.add_argument("--calories", required=True, help="target calories for the day")
    s.add_argument("--meals", default=3, help="meals per day (default: 3)")
    s.add_argument("--exclude", default="", help="comma-separated exclusions (e.g., noten,pinda)")
    s.add_argument("--date", default="today", help="ISO date or 'today'")
    s.add_argument("--bias-tags", action="store_true", help="bias meal selection by typical tags (ontbijt/lunch/diner)")
    s.set_defaults(func=cmd_plan_day)

    s = sub.add_parser("plan-week", help="generate and save meal plans for multiple days")
    s.add_argument("--start", default="today", help="ISO start date or 'today'")
    s.add_argument("--days", type=int, default=7, help="number of days to plan (default: 7)")
    s.add_argument("--calories", required=True, help="target calories per day")
    s.add_argument("--protein", type=float, help="target grams of protein per day")
    s.add_argument("--carbs", type=float, help="target grams of carbs per day")
    s.add_argument("--fat", type=float, help="target grams of fat per day")
    s.add_argument("--meals", default=3, help="meals per day (default: 3)")
    s.add_argument("--exclude", default="", help="comma-separated exclusions (e.g., noten,pinda)")
    s.add_argument("--bias-tags", action="store_true", help="bias meal selection by typical tags (ontbijt/lunch/diner)")
    s.set_defaults(func=cmd_plan_week)

    s = sub.add_parser("crawl-allerhande", help="crawl and ingest many Allerhande recipes via sitemaps")
    s.add_argument("--limit", type=int, default=200, help="max recipes to ingest")
    s.add_argument("--sitemap", action="append", help="seed sitemap URL(s)")
    s.set_defaults(func=cmd_crawl_allerhande)

    s = sub.add_parser("crawl-ah-products", help="crawl and ingest many AH products via sitemaps")
    s.add_argument("--limit", type=int, default=200, help="max products to ingest")
    s.add_argument("--sitemap", action="append", help="seed sitemap URL(s)")
    s.set_defaults(func=cmd_crawl_ah_products)

    s = sub.add_parser("crawl-etm-foods", help="ingest foods from the EatThisMuch API")
    s.add_argument("--limit", type=int, default=100, help="max foods to ingest")
    s.add_argument("--delay", type=float, default=0.2, help="delay between API calls in seconds")
    s.set_defaults(func=cmd_crawl_etm_foods)

    s = sub.add_parser("crawl-etm-recipes", help="ingest recipes from the EatThisMuch API")
    s.add_argument("--limit", type=int, default=100, help="max recipes to ingest")
    s.add_argument("--delay", type=float, default=0.2, help="delay between API calls in seconds")
    s.set_defaults(func=cmd_crawl_etm_recipes)

    s = sub.add_parser("refresh-nutrition", help="refresh nutrition fields for recipes (parse from recipe pages)")
    s.add_argument("--limit", type=int, default=2000, help="max recipes to update")
    s.add_argument("--missing-only", action="store_true", help="only update recipes with missing/zero values")
    def _cmd_refresh(args):
        updated = 0
        with db.connect() as conn:
            db.init_db(conn)
            # Select candidates
            if args.missing_only:
                rows = conn.execute(
                    """
                    select id, url from recipes
                    where (kcal_per_serving is null or kcal_per_serving=0)
                       or (protein_g_per_serving is null or protein_g_per_serving=0)
                       or (carbs_g_per_serving is null or carbs_g_per_serving=0)
                       or (fat_g_per_serving is null or fat_g_per_serving=0)
                       or (fiber_g_per_serving is null or fiber_g_per_serving=0)
                      and url like 'http%'
                    order by id desc
                    limit ?
                    """,
                    (args.limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "select id, url from recipes where url like 'http%' order by id desc limit ?",
                    (args.limit,)
                ).fetchall()
            cur = conn.cursor()
            for r in rows:
                rid, url = r["id"], r["url"]
                if not url:
                    continue
                try:
                    status, data = fetch(url, delay_s=0.5)
                    if status != 200:
                        continue
                    html = data.decode('utf-8','ignore')
                    jld = _extract_json_ld(html)
                    recipe_obj = _first_recipe(jld) if jld else None
                    nut = _extract_nutrition_from_jsonld(recipe_obj) if recipe_obj else {}
                    kcal = nut.get('kcal')
                    pr = nut.get('protein')
                    cb = nut.get('carbs')
                    ft = nut.get('fat')
                    fb = nut.get('fiber')
                    if any(x is None for x in (kcal, pr, cb, ft, fb)):
                        hn = _extract_nutrition_from_html(html)
                        kcal = kcal if kcal is not None else hn.get('kcal')
                        pr = pr if pr is not None else hn.get('protein')
                        cb = cb if cb is not None else hn.get('carbs')
                        ft = ft if ft is not None else hn.get('fat')
                        fb = fb if fb is not None else hn.get('fiber')
                    if any(x is not None for x in (kcal, pr, cb, ft, fb)):
                        cur.execute(
                            "update recipes set kcal_per_serving=?, protein_g_per_serving=?, carbs_g_per_serving=?, fat_g_per_serving=?, fiber_g_per_serving=? where id=?",
                            (kcal, pr, cb, ft, fb, rid)
                        )
                        updated += 1
                except Exception:
                    continue
        print(f"Updated {updated} recipe(s)")
    s.set_defaults(func=_cmd_refresh)

    return p


def default_meal_tags(meals: int) -> List[List[str]]:
    meals = int(meals)
    slots: List[List[str]] = []
    if meals >= 1:
        slots.append(["ontbijt", "breakfast"])
    if meals >= 2:
        slots.append(["lunch", "brood", "salade"])
    if meals >= 3:
        slots.append(["diner", "avondeten", "hoofdgerecht", "maaltijd"])
    for _ in range(3, meals):
        slots.append([])  # no bias for extra meals/snacks
    return slots


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "cmd", None):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
