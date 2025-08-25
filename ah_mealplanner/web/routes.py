import os
import json
import re
import threading
import time
from collections import deque
from datetime import date
from flask import Blueprint, render_template, request, redirect, url_for, flash

from .. import db
from ..meal_planner import generate_daily_plan, generate_weekly_plan
from ..ingest_allerhande import fetch_recipe, crawl_allerhande, enrich_recipe, refresh_nutrition
from ..ingest_eatthismuch import crawl_etm_foods, crawl_etm_recipes
# Product crawl and verification disabled


bp = Blueprint("core", __name__, template_folder="templates")


def _conn():
    return db.connect(os.environ.get("AH_MEALPLANNER_DB"))


def default_meal_tags_web(meals: int):
    # Reuse CLI default to avoid duplication in web.
    try:
        from ..cli import default_meal_tags
        return default_meal_tags(meals)
    except Exception:
        # Fallback in case of import issues
        meals = int(meals)
        slots: list[list[str]] = []
        if meals >= 1:
            slots.append(["ontbijt", "breakfast"])
        if meals >= 2:
            slots.append(["lunch", "brood", "salade"])
        if meals >= 3:
            slots.append(["diner", "avondeten", "hoofdgerecht", "maaltijd"])
        for _ in range(3, meals):
            slots.append([])
        return slots


@bp.app_template_filter("fmt")
def fmt_number(v):
    try:
        return f"{float(v):.1f}"
    except Exception:
        return v


@bp.route("/")
def home():
    with _conn() as conn:
        db.init_db(conn)
        n_rec = conn.execute("select count(*) from recipes").fetchone()[0]
        n_prod = conn.execute("select count(*) from products").fetchone()[0]
        last_plans = conn.execute("select date, total_calories from meal_plans order by date desc limit 7").fetchall()
    return render_template("home.html", n_rec=n_rec, n_prod=n_prod, last_plans=last_plans)


@bp.route("/recipes")
def recipes():
    q = request.args.get("q", "").strip()
    tag = request.args.get("tag", "").strip()
    with _conn() as conn:
        db.init_db(conn)
        # Fetch recipe rows based on filters (support combined q+tag)
        from typing import Optional
        def _fts_query(s: str) -> Optional[str]:
            tokens = re.findall(r"\w+", s)
            if not tokens:
                return None
            return " AND ".join([t + "*" for t in tokens])

        if q and tag:
            match = _fts_query(q)
            rows = []
            if match:
                try:
                    rows = conn.execute(
                        """
                        select distinct r.*
                        from recipes r
                        join recipes_fts f on f.rowid = r.id
                        join recipe_tags t on r.id = t.recipe_id
                        where recipes_fts match ? and lower(t.tag) = lower(?)
                        order by bm25(recipes_fts), r.id desc
                        limit 100
                        """,
                        (match, tag),
                    ).fetchall()
                except Exception:
                    pass
            if not rows:
                rows = conn.execute(
                    "select distinct r.* from recipes r join recipe_tags t on r.id=t.recipe_id where r.title like ? and lower(t.tag)=lower(?) order by r.id desc limit 100",
                    (f"%{q}%", tag),
                ).fetchall()
        elif tag:
            rows = conn.execute(
                "select distinct r.* from recipes r join recipe_tags t on r.id=t.recipe_id where lower(t.tag)=lower(?) order by r.id desc limit 100",
                (tag,),
            ).fetchall()
        elif q:
            match = _fts_query(q)
            rows = []
            if match:
                try:
                    rows = conn.execute(
                        """
                        select r.*
                        from recipes r
                        join recipes_fts f on f.rowid = r.id
                        where recipes_fts match ?
                        order by bm25(recipes_fts), r.id desc
                        limit 50
                        """,
                        (match,),
                    ).fetchall()
                except Exception:
                    pass
            if not rows:
                rows = conn.execute(
                    "select * from recipes where title like ? order by id desc limit 50",
                    (f"%{q}%",),
                ).fetchall()
        else:
            rows = conn.execute("select * from recipes order by id desc limit 50").fetchall()
        # Compute tag facets (tag -> count of recipes). When searching by title, restrict counts to matching recipes; otherwise global.
        if q:
            match = _fts_query(q)
            if match:
                sql = (
                    """
                    select t.tag as tag, t.tag_type as tag_type, count(distinct t.recipe_id) as cnt
                    from recipe_tags t
                    join recipes_fts f on f.rowid = t.recipe_id
                    where recipes_fts match ?
                    group by t.tag_type, t.tag
                    order by (t.tag_type is null), t.tag_type asc, cnt desc, tag asc
                    limit 400
                    """
                )
                params = (match,)
            else:
                sql = (
                    """
                    select t.tag as tag, t.tag_type as tag_type, count(distinct t.recipe_id) as cnt
                    from recipe_tags t
                    join recipes r on r.id = t.recipe_id
                    where r.title like ?
                    group by t.tag_type, t.tag
                    order by (t.tag_type is null), t.tag_type asc, cnt desc, tag asc
                    limit 400
                    """
                )
                params = (f"%{q}%",)
            tag_facets = conn.execute(sql, params).fetchall()
        else:
            tag_facets = conn.execute(
                """
                select t.tag as tag, t.tag_type as tag_type, count(distinct t.recipe_id) as cnt
                from recipe_tags t
                group by t.tag_type, t.tag
                order by (t.tag_type is null), t.tag_type asc, cnt desc, tag asc
                limit 400
                """
            ).fetchall()
    tag_facets = [f for f in tag_facets if f["tag_type"]]
    return render_template("recipes.html", recipes=rows, q=q, tag=tag, tag_facets=tag_facets)


@bp.route("/recipes/<int:rid>")
def recipe_detail(rid: int):
    with _conn() as conn:
        db.init_db(conn)
        r = conn.execute("select * from recipes where id=?", (rid,)).fetchone()
        ings = conn.execute(
            """
            select i.*, p.name as product_name, p.url as product_url
            from ingredients i
            left join products p on i.product_id = p.id
            where i.recipe_id=?
            """,
            (rid,),
        ).fetchall()
        tags = conn.execute("select tag, tag_type from recipe_tags where recipe_id=? order by tag_type, tag", (rid,)).fetchall()
    if not r:
        return ("Not found", 404)
    src_url = _canonical_recipe_url(r)
    return render_template("recipe_detail.html", r=r, ings=ings, src_url=src_url, tags=tags)


@bp.route("/settings", methods=["GET", "POST"])
def settings():
    if request.method == "POST":
        with _conn() as conn:
            db.save_setting(conn, 'macro_p', request.form.get('protein_pct', '30'))
            db.save_setting(conn, 'macro_c', request.form.get('carbs_pct', '40'))
            db.save_setting(conn, 'macro_f', request.form.get('fat_pct', '30'))
            db.save_setting(conn, 'default_servings', request.form.get('default_servings', '1.0'))
            flash("Settings saved.")
        return redirect(url_for("core.settings"))
    
    with _conn() as conn:
        settings = {
            'p': db.get_setting(conn, 'macro_p', '30'),
            'c': db.get_setting(conn, 'macro_c', '40'),
            'f': db.get_setting(conn, 'macro_f', '30'),
            'servings': db.get_setting(conn, 'default_servings', '1.0')
        }
    return render_template("settings.html", settings=settings)


@bp.route("/recipes/<int:rid>/verify-products")
def recipe_verify_products(rid: int):
    return ("Product verification disabled", 404)


from typing import Optional
def _canonical_recipe_url(r) -> Optional[str]:
    # Prefer stored absolute URL
    u = r["url"]
    if u and isinstance(u, str) and (u.startswith("http://") or u.startswith("https://")):
        return u
    # Try JSON-LD fields
    raw = r["raw_json"]
    if raw:
        try:
            j = json.loads(raw)
            # JSON-LD may expose url or mainEntityOfPage
            for key in ("url", "@id"):
                v = j.get(key)
                if isinstance(v, str) and v.startswith("http"):
                    return v
            mep = j.get("mainEntityOfPage")
            if isinstance(mep, dict):
                for key in ("@id", "url"):
                    v = mep.get(key)
                    if isinstance(v, str) and v.startswith("http"):
                        return v
        except Exception:
            pass
    # Construct from source/id for Allerhande
    if (r.get("source") == "allerhande"):
        sid = r.get("source_id")
        if not sid and r.get("url"):
            m = re.search(r"/allerhande/recept/([^/]+)", r.get("url"))
            if m:
                sid = m.group(1)
        if sid:
            return f"https://www.ah.nl/allerhande/recept/{sid}"
    return None


# Products page removed


@bp.route("/plan", methods=["GET", "POST"])
def plan():
    if request.method == "POST":
        calories = float(request.form.get("calories", 2200))
        meals = int(request.form.get("meals", 3))
        date_str = request.form.get("date") or date.today().isoformat()
        exclude = request.form.get("exclude", "").strip()
        exclusions = [x.strip() for x in exclude.split(",") if x.strip()]
        # Optional macro goals
        def _to_float(name):
            try:
                v = request.form.get(name)
                return float(v) if v is not None and v != "" else None
            except Exception:
                return None
        macro_targets = {
            "protein_g": _to_float("protein_g"),
            "carbs_g": _to_float("carbs_g"),
            "fat_g": _to_float("fat_g"),
        }
        # Build preferred tags per selected meal slots, overriding meals count if provided
        selected_slots = []
        slot_names = []
        if request.form.get("slot_breakfast"):
            selected_slots.append(["ontbijt", "breakfast"])
            slot_names.append("Breakfast")
        if request.form.get("slot_lunch"):
            selected_slots.append(["lunch", "brood", "salade"])
            slot_names.append("Lunch")
        if request.form.get("slot_dinner"):
            selected_slots.append(["diner", "avondeten", "hoofdgerecht", "maaltijd"])
            slot_names.append("Dinner")
        if request.form.get("slot_snack"):
            selected_slots.append(["snack", "tussendoor", "borrel"])
            slot_names.append("Snack")
        preferred_slots = selected_slots if selected_slots else (default_meal_tags_web(meals) if request.form.get("bias_tags") else None)
        if selected_slots:
            meals = len(selected_slots)

        with _conn() as conn:
            db.init_db(conn)
            preferred = preferred_slots
            pid, items, totals = generate_daily_plan(
                conn,
                target_calories=calories,
                meals_per_day=meals,
                macro_targets=macro_targets,
                exclusions=exclusions,
                preferred_tags_per_meal=preferred,
                slot_names=slot_names if selected_slots else None,
                date=date_str,
            )
        return redirect(url_for("core.view_plan", date=date_str))
    return render_template("plan.html", today=date.today().isoformat())


@bp.route("/plan/<date>")
def view_plan(date: str):
    with _conn() as conn:
        db.init_db(conn)
        plan_row = conn.execute("select * from meal_plans where date=?", (date,)).fetchone()
        if not plan_row:
            flash("No plan found for this date.")
            return redirect(url_for("core.plan"))
        # Convert to plain dict and parse macros_json for safe template access
        plan = dict(plan_row)
        try:
            plan["macros_json"] = json.loads(plan.get("macros_json") or "null")
        except Exception:
            plan["macros_json"] = None
        items = conn.execute("select * from meal_plan_items where meal_plan_id=? order by meal_index", (plan["id"],)).fetchall()
    # Resolve item titles via notes fallback
    resolved = []
    with _conn() as conn:
        for it in items:
            title = it["notes"] or ""
            if it["item_type"] == "recipe":
                r = conn.execute("select title, kcal_per_serving from recipes where id=?", (it["item_id"],)).fetchone()
                if r:
                    title = title or r["title"]
            else:
                p = conn.execute("select name, kcal_per_100 from products where id=?", (it["item_id"],)).fetchone()
                if p:
                    title = title or p["name"]
            resolved.append((it, title))
    return render_template("plan_result.html", plan=plan, items=resolved)


def _recalculate_plan_totals(conn, plan_id):
    # Recalculate all totals for the plan
    plan_items = conn.execute("SELECT * FROM meal_plan_items WHERE meal_plan_id=?", (plan_id,)).fetchall()
    
    total_kcal = 0
    total_p = 0
    total_c = 0
    total_f = 0

    for item in plan_items:
        item_servings = item['servings']
        if item['item_type'] == 'recipe':
            nutri = conn.execute("SELECT kcal_per_serving, protein_g_per_serving, carbs_g_per_serving, fat_g_per_serving FROM recipes WHERE id=?", (item['item_id'],)).fetchone()
            if nutri:
                total_kcal += (nutri['kcal_per_serving'] or 0) * item_servings
                total_p += (nutri['protein_g_per_serving'] or 0) * item_servings
                total_c += (nutri['carbs_g_per_serving'] or 0) * item_servings
                total_f += (nutri['fat_g_per_serving'] or 0) * item_servings
        elif item['item_type'] == 'product':
            nutri = conn.execute("SELECT kcal_per_100, protein_g_per_100, carbs_g_per_100, fat_g_per_100 FROM products WHERE id=?", (item['item_id'],)).fetchone()
            if nutri:
                factor = item_servings # servings for products are stored as 100g units
                total_kcal += (nutri['kcal_per_100'] or 0) * factor
                total_p += (nutri['protein_g_per_100'] or 0) * factor
                total_c += (nutri['carbs_g_per_100'] or 0) * factor
                total_f += (nutri['fat_g_per_100'] or 0) * factor
    
    # Update the parent meal_plan row
    conn.execute(
        "UPDATE meal_plans SET total_calories=?, total_protein=?, total_carbs=?, total_fat=? WHERE id=?",
        (total_kcal, total_p, total_c, total_f, plan_id)
    )

@bp.route("/plan/item/<int:item_id>/servings", methods=["POST"])
def update_servings(item_id: int):
    servings = float(request.form.get("servings", 1.0))
    date = request.args.get("date")
    if not date:
        flash("Date is missing.", "error")
        return redirect(url_for("core.home"))

    with _conn() as conn:
        # Update the serving size for the specific item
        conn.execute("UPDATE meal_plan_items SET servings=? WHERE id=?", (servings, item_id))
        
        # Get the plan_id from the item
        item_row = conn.execute("SELECT meal_plan_id FROM meal_plan_items WHERE id=?", (item_id,)).fetchone()
        if not item_row:
            flash("Plan item not found.", "error")
            return redirect(url_for("core.view_plan", date=date))
        plan_id = item_row['meal_plan_id']

        _recalculate_plan_totals(conn, plan_id)

    return redirect(url_for("core.view_plan", date=date))


@bp.route("/ingest", methods=["GET", "POST"])
def ingest():
    if request.method == "POST":
        url = (request.form.get("recipe_url") or "").strip()
        if not url:
            flash("Please provide a recipe URL.")
            return redirect(url_for("core.ingest"))
        try:
            recipe_row, ingredients, tags = fetch_recipe(url)
            with _conn() as conn:
                db.init_db(conn)
                rid = db.insert_recipe(conn, recipe_row, ingredients)
                try:
                    db.insert_recipe_tags(conn, rid, tags)
                except Exception:
                    pass
                # Parse quantities, link products, compute nutrition
                try:
                    res = enrich_recipe(conn, rid, url, recipe_row.get('servings'), None, link_products=False, compute_nutrition=False)
                    if res.get('parsed'):
                        flash(f"Parsed {res['parsed']} ingredients")
                    # Product linking/computation disabled
                except Exception as e:
                    flash(f"Enrichment failed: {e}")
            flash(f"Ingested recipe: {recipe_row.get('title')}")
            return redirect(url_for("core.recipe_detail", rid=rid))
        except Exception as e:
            flash(f"Failed to ingest recipe: {e}")
            return redirect(url_for("core.ingest"))
    return render_template("ingest.html")


@bp.route("/plan-week", methods=["GET", "POST"])
def plan_week():
    if request.method == "POST":
        calories = float(request.form.get("calories", 2200))
        meals = int(request.form.get("meals", 3))
        start = request.form.get("start") or date.today().isoformat()
        days = int(request.form.get("days", 7))
        exclude = request.form.get("exclude", "").strip()
        exclusions = [x.strip() for x in exclude.split(",") if x.strip()]
        # Optional macro goals
        def _to_float(name):
            try:
                v = request.form.get(name)
                return float(v) if v is not None and v != "" else None
            except Exception:
                return None
        macro_targets = {
            "protein_g": _to_float("protein_g"),
            "carbs_g": _to_float("carbs_g"),
            "fat_g": _to_float("fat_g"),
        }
        # Build preferred tags per selected meal slots
        selected_slots = []
        slot_names = []
        if request.form.get("slot_breakfast"):
            selected_slots.append(["ontbijt", "breakfast"])
            slot_names.append("Breakfast")
        if request.form.get("slot_lunch"):
            selected_slots.append(["lunch", "brood", "salade"])
            slot_names.append("Lunch")
        if request.form.get("slot_dinner"):
            selected_slots.append(["diner", "avondeten", "hoofdgerecht", "maaltijd"])
            slot_names.append("Dinner")
        if request.form.get("slot_snack"):
            selected_slots.append(["snack", "tussendoor", "borrel"])
            slot_names.append("Snack")
        preferred_slots = selected_slots if selected_slots else (default_meal_tags_web(meals) if request.form.get("bias_tags") else None)
        if selected_slots:
            meals = len(selected_slots)

        with _conn() as conn:
            db.init_db(conn)
            preferred = preferred_slots
            generate_weekly_plan(
                conn,
                start,
                days=days,
                target_calories=calories,
                meals_per_day=meals,
                macro_targets=macro_targets,
                exclusions=exclusions,
                preferred_tags_per_meal=preferred,
            )
        return redirect(url_for("core.shopping_list", start=start, days=days))
    return render_template("plan_week.html", today=date.today().isoformat())


@bp.route("/shopping-list")
def shopping_list():
    start = request.args.get("start") or date.today().isoformat()
    days = int(request.args.get("days", 7))
    from datetime import datetime, timedelta
    d0 = datetime.fromisoformat(start).date()
    d1 = d0 + timedelta(days=days)
    dates = [(d0 + timedelta(days=i)).isoformat() for i in range(days)]
    with _conn() as conn:
        db.init_db(conn)
        # Collect plan ids for the date range
        rows = conn.execute("select id, date from meal_plans where date >= ? and date < ? order by date", (d0.isoformat(), d1.isoformat())).fetchall()
        ids = [r["id"] for r in rows]
        items = []
        if ids:
            qmarks = ",".join(["?"] * len(ids))
            items = conn.execute(f"select * from meal_plan_items where meal_plan_id in ({qmarks})", ids).fetchall()
        # Aggregate
        ingredients_map = {}  # (name, unit) -> quantity
        ingredients_free = {}  # raw string -> count
        products_map = {}  # product_id -> grams
        for it in items:
            if it["item_type"] == "recipe":
                ings = conn.execute("select name, quantity, unit, raw from ingredients where recipe_id=?", (it["item_id"],)).fetchall()
                for ing in ings:
                    name = (ing["name"] or "").strip().lower()
                    qty = ing["quantity"]
                    unit = (ing["unit"] or "").strip().lower() or ""
                    if qty is not None and unit:
                        key = (name, unit)
                        ingredients_map[key] = ingredients_map.get(key, 0.0) + float(qty)
                    else:
                        raw = (ing["raw"] or ing["name"] or "").strip()
                        if raw:
                            ingredients_free[raw] = ingredients_free.get(raw, 0) + 1
            else:
                grams = (it["servings"] or 0.0) * 100.0
                products_map[it["item_id"]] = products_map.get(it["item_id"], 0.0) + grams
        # Resolve product names
        products_list = []
        for pid, grams in products_map.items():
            p = conn.execute("select name, url from products where id=?", (pid,)).fetchone()
            name = p["name"] if p else f"product #{pid}"
            url = p["url"] if p else None
            products_list.append((name, grams, url))
        products_list.sort(key=lambda x: x[0])
        # Sort ingredients
        structured_ings = [ (n, u, q) for (n, u), q in ingredients_map.items() ]
        structured_ings.sort(key=lambda x: x[0])
        free_ings = sorted(ingredients_free.items(), key=lambda x: (-x[1], x[0]))
    return render_template(
        "shopping_list.html",
        start=start,
        days=days,
        dates=dates,
        ingredients_structured=structured_ings,
        ingredients_free=free_ings,
        products=products_list,
    )


# Admin: trigger crawls and monitor progress
_crawl_state = {
    "recipes": {"running": False, "processed": 0, "errors": 0, "started_at": None, "finished_at": None, "last": None},
    "nutrition": {"running": False, "processed": 0, "errors": 0, "started_at": None, "finished_at": None, "last": None},
    "etm_foods": {"running": False, "processed": 0, "errors": 0, "started_at": None, "finished_at": None, "last": None},
    "etm_recipes": {"running": False, "processed": 0, "errors": 0, "started_at": None, "finished_at": None, "last": None},
}
_crawl_logs = {
    "recipes": deque(maxlen=200),
    "nutrition": deque(maxlen=200),
    "etm_foods": deque(maxlen=200),
    "etm_recipes": deque(maxlen=200),
}
_crawl_lock = threading.Lock()


def _start_crawl(job: str, seeds: list[str], limit: int, delay: float | None = None):
    state = _crawl_state[job]
    with _crawl_lock:
        if state["running"]:
            return False
        state.update({"running": True, "processed": 0, "errors": 0, "started_at": time.time(), "finished_at": None, "last": None})
        _crawl_logs[job].clear()

    def progress(ev):
        ev = dict(ev)
        ev["ts"] = time.time()
        with _crawl_lock:
            if ev.get("status") == "ok":
                state["processed"] += 1
            else:
                state["errors"] += 1
                state["last"] = ev.get("error")
            _crawl_logs[job].appendleft(ev)

    def run():
        try:
            with _conn() as conn:
                db.init_db(conn)
                if job == "recipes":
                    crawl_allerhande(conn, seeds, limit=limit, delay_s=1.0, progress=progress)
                elif job == "nutrition":
                    # seeds carries flags in this context: first line 'missing_only=true/false'
                    missing_only = True
                    if seeds:
                        first = seeds[0].strip().lower()
                        if first in ("missing_only=false", "missing=false", "all"):
                            missing_only = False
                    def nprogress(ev):
                        ev = dict(ev)
                        ev["ts"] = time.time()
                        with _crawl_lock:
                            st_n = _crawl_state["nutrition"]
                            if ev.get("status") == "ok":
                                st_n["processed"] += 1
                            else:
                                st_n["errors"] += 1
                                st_n["last"] = ev.get("error")
                            _crawl_logs["nutrition"].appendleft(ev)
                    refresh_nutrition(conn, limit=limit, missing_only=missing_only, delay_s=0.5, progress=nprogress)
                elif job == "etm_foods":
                    crawl_etm_foods(conn, limit=limit, delay_s=delay or 0.2, progress=progress)
                elif job == "etm_recipes":
                    crawl_etm_recipes(conn, limit=limit, delay_s=delay or 0.2, progress=progress)
                else:
                    # Product crawl disabled
                    pass
        finally:
            with _crawl_lock:
                state["running"] = False
                state["finished_at"] = time.time()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return True


@bp.route("/admin", methods=["GET", "POST"])
def admin():
    message = None
    if request.method == "POST":
        job = request.form.get("job")
        limit = int(request.form.get("limit", 100))
        delay = float(request.form.get("delay", 0.2))
        sitemaps = request.form.get("sitemaps", "").strip()
        seeds = [s.strip() for s in sitemaps.replace("\r", "\n").split("\n") if s.strip()]
        if job == "wipe":
            confirm = (request.form.get("confirm") or "").strip()
            if confirm != "DELETE":
                message = "Type DELETE to confirm wipe. Nothing removed."
            else:
                with _conn() as conn:
                    db.init_db(conn)
                    cur = conn.cursor()
                    # Count rows before deleting
                    tables = [
                        "meal_plan_items",
                        "meal_plans",
                        "ingredients",
                        "recipe_tags",
                        "recipes",
                        "products",
                        "seen_pages",
                    ]
                    counts = {}
                    for t in tables:
                        try:
                            counts[t] = cur.execute(f"select count(*) from {t}").fetchone()[0]
                        except Exception:
                            counts[t] = 0
                    # Delete in safe order (children first)
                    for t in tables:
                        try:
                            cur.execute(f"delete from {t}")
                        except Exception:
                            pass
                    # Reset autoincrement sequences
                    try:
                        cur.execute("delete from sqlite_sequence")
                    except Exception:
                        pass
                message = (
                    "Removed data: "
                    f"recipes={counts.get('recipes',0)}, products={counts.get('products',0)}, "
                    f"ingredients={counts.get('ingredients',0)}, tags={counts.get('recipe_tags',0)}, "
                    f"plans={counts.get('meal_plans',0)} items={counts.get('meal_plan_items',0)}"
                )
        elif job == "nutrition":
            missing_only = "true" if request.form.get("missing_only") else "false"
            # Use seeds param as a carrier for flags (to reuse _start_crawl signature)
            started = _start_crawl("nutrition", [f"missing_only={missing_only}"], limit)
            message = f"Started nutrition refresh (missing_only={missing_only}) with limit {limit}" if started else "Nutrition refresh already running"
        elif job in ("etm_foods", "etm_recipes"):
            started = _start_crawl(job, [], limit, delay)
            name = "ETM foods" if job == "etm_foods" else "ETM recipes"
            message = f"Started {name} crawl with limit {limit}" if started else f"{name} crawl already running"
        else:
            if not seeds:
                if job == "recipes":
                    seeds = ["https://www.ah.nl/sitemaps/entities/allerhande/recipes.xml"]
                else:
                    seeds = ["https://www.ah.nl/sitemaps/entities/products/detail.xml"]
            started = _start_crawl(job, seeds, limit)
            message = f"Started {job} crawl with limit {limit}" if started else f"{job} crawl already running"

    with _conn() as conn:
        db.init_db(conn)
        stats = {
            "recipes": conn.execute("select count(*) c from recipes").fetchone()[0],
            "seen_ok_recipes": conn.execute("select count(*) from seen_pages where type='allerhande_recipe' and status='ok'").fetchone()[0],
        }
        errors = conn.execute("select url, last_error, last_seen from seen_pages where status='error' order by last_seen desc limit 20").fetchall()
    return render_template("admin.html", stats=stats, errors=errors, state=_crawl_state, message=message)


@bp.route("/admin/state")
def admin_state():
    # Lightweight JSON endpoint for polling progress/logs
    def _ser_state(job):
        with _crawl_lock:
            s = dict(_crawl_state[job])
            s["events"] = list(_crawl_logs[job])[:50]
        return s
    return {
        "recipes": _ser_state("recipes"),
        "nutrition": _ser_state("nutrition"),
        "etm_foods": _ser_state("etm_foods"),
        "etm_recipes": _ser_state("etm_recipes"),
    }


@bp.route("/admin/recent")
def admin_recent():
    # Return latest items so the admin page can render new rows as they are committed
    with _conn() as conn:
        db.init_db(conn)
        rec = conn.execute(
            "select id, title, date(last_seen) as seen, last_seen from recipes order by id desc limit 20"
        ).fetchall()
        prod = conn.execute(
            "select id, name, date(last_seen) as seen, last_seen from products order by id desc limit 20"
        ).fetchall()
    def ser(rows, keys=("id","title","seen","last_seen")):
        out = []
        for r in rows:
            d = {k: r[k] if k in r.keys() else None for k in keys}
            if "title" not in d:  # products
                d["title"] = r["name"]
            out.append(d)
        return out
    return {"recipes": ser(rec), "products": ser(prod)}


@bp.route("/alternatives/<int:item_id>/<int:plan_id>", methods=["GET", "POST"])
def alternatives(item_id: int, plan_id: int):
    if request.method == "POST":
        new_recipe_id = request.form.get("recipe_id")
        if not new_recipe_id:
            return ("No recipe selected", 400)
        try:
            new_recipe_id = int(new_recipe_id)
        except ValueError:
            return ("Invalid recipe id", 400)
        with _conn() as conn:
            conn.execute("update meal_plan_items set item_id = ? where id = ?", (new_recipe_id, item_id))
            _recalculate_plan_totals(conn, plan_id)
            plan = conn.execute("select date from meal_plans where id = ?", (plan_id,)).fetchone()
        return redirect(url_for("core.view_plan", date=plan['date']))

    with _conn() as conn:
        db.init_db(conn)
        # Get the original recipe
        item = conn.execute("select * from meal_plan_items where id=?", (item_id,)).fetchone()
        r = conn.execute("select * from recipes where id=?", (item['item_id'],)).fetchone()
        if not r:
            return ("Recipe not found", 404)

        # Find recipes with similar tags
        tags = conn.execute("select tag from recipe_tags where recipe_id=?", (r['id'],)).fetchall()
        if not tags:
            # No tags, so no alternatives based on tags
            return render_template("alternatives.html", recipe=r, alternatives=[])

        tag_names = [t['tag'] for t in tags]
        qmarks = ",".join(["?"] * len(tag_names))
        similar_recipes = conn.execute(f"""
            select r.* from recipes r
            join recipe_tags t on r.id = t.recipe_id
            where t.tag in ({qmarks}) and r.id != ?
            group by r.id
            order by count(r.id) desc
            limit 10
        """, (*tag_names, r['id'])).fetchall()

    return render_template("alternatives.html", recipe=r, alternatives=similar_recipes, plan_id=plan_id, item_id=item_id)

