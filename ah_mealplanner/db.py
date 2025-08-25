import json
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Dict, Iterable, List, Optional


DB_PATH = os.environ.get("AH_MEALPLANNER_DB", os.path.join("data", "mealplanner.db"))


def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


@contextmanager
def connect(db_path: Optional[str] = None):
    path = db_path or DB_PATH
    _ensure_parent(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception:
        pass
    try:
        conn.execute("PRAGMA busy_timeout=15000")
    except Exception:
        pass
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Products from Albert Heijn
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ah_id TEXT UNIQUE,
            name TEXT NOT NULL,
            brand TEXT,
            category TEXT,
            unit TEXT,
            price_eur REAL,
            kcal_per_100 REAL,
            protein_g_per_100 REAL,
            carbs_g_per_100 REAL,
            fat_g_per_100 REAL,
            fiber_g_per_100 REAL,
            salt_g_per_100 REAL,
            nutrition_json TEXT,
            url TEXT,
            image_url TEXT,
            last_seen TEXT
        )
        """
    )

    # Recipes (e.g., from Allerhande)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            source_id TEXT,
            title TEXT NOT NULL,
            url TEXT,
            image_url TEXT,
            servings INTEGER,
            total_time_min INTEGER,
            kcal_per_serving REAL,
            protein_g_per_serving REAL,
            carbs_g_per_serving REAL,
            fat_g_per_serving REAL,
            fiber_g_per_serving REAL,
            instructions TEXT,
            raw_json TEXT,
            last_seen TEXT
        )
        """
    )

    # Ingredients linked to recipes
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ingredients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            quantity REAL,
            unit TEXT,
            product_id INTEGER,
            raw TEXT,
            FOREIGN KEY(recipe_id) REFERENCES recipes(id),
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
        """
    )

    # Recipe tags (themes/categories/keywords)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS recipe_tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id INTEGER NOT NULL,
            tag TEXT NOT NULL,
            tag_type TEXT,
            FOREIGN KEY(recipe_id) REFERENCES recipes(id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS ix_recipe_tags_tag ON recipe_tags(tag)")

    # Helpful indexes for performance
    cur.execute("CREATE INDEX IF NOT EXISTS ix_recipes_title ON recipes(title)")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_ingredients_recipe_id ON ingredients(recipe_id)")

    # Meal plans
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meal_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT UNIQUE,
            target_calories REAL,
            meals_per_day INTEGER,
            macros_json TEXT,
            total_calories REAL,
            total_protein REAL,
            total_carbs REAL,
            total_fat REAL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meal_plan_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meal_plan_id INTEGER NOT NULL,
            meal_index INTEGER NOT NULL,
            item_type TEXT NOT NULL, -- 'recipe' or 'product'
            item_id INTEGER NOT NULL,
            servings REAL DEFAULT 1.0,
            notes TEXT,
            FOREIGN KEY(meal_plan_id) REFERENCES meal_plans(id)
        )
        """
    )

    conn.commit()

    # Full-text search for recipes (title + instructions)
    try:
        cur.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS recipes_fts
            USING fts5(title, instructions, content='recipes', content_rowid='id')
            """
        )
        # Triggers to keep FTS in sync
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS recipes_ai AFTER INSERT ON recipes BEGIN
                INSERT INTO recipes_fts(rowid, title, instructions) VALUES (new.id, new.title, new.instructions);
            END;
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS recipes_ad AFTER DELETE ON recipes BEGIN
                INSERT INTO recipes_fts(recipes_fts, rowid) VALUES ('delete', old.id);
            END;
            """
        )
        cur.execute(
            """
            CREATE TRIGGER IF NOT EXISTS recipes_au AFTER UPDATE ON recipes BEGIN
                INSERT INTO recipes_fts(recipes_fts, rowid) VALUES ('delete', old.id);
                INSERT INTO recipes_fts(rowid, title, instructions) VALUES (new.id, new.title, new.instructions);
            END;
            """
        )
        # Populate FTS if empty
        try:
            n = cur.execute("SELECT count(*) FROM recipes_fts").fetchone()[0]
        except Exception:
            n = 0
        if not n:
            try:
                cur.execute("INSERT INTO recipes_fts(recipes_fts) VALUES ('rebuild')")
            except Exception:
                pass
        conn.commit()
    except Exception:
        # FTS5 may not be available; ignore silently
        pass

    # Crawl state tables (for resumable scraping)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_pages (
            url TEXT PRIMARY KEY,
            type TEXT,
            status TEXT,
            http_status INTEGER,
            last_error TEXT,
            last_seen TEXT
        )
        """
    )
    conn.commit()

    # App settings
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    # Default settings
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('macro_p', '30'))
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('macro_c', '40'))
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('macro_f', '30'))
    cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('default_servings', '1.0'))


def get_setting(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def save_setting(conn: sqlite3.Connection, key: str, value: str):
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))


def upsert_product(conn: sqlite3.Connection, product: Dict) -> int:
    cur = conn.cursor()
    fields = (
        "ah_id", "name", "brand", "category", "unit", "price_eur",
        "kcal_per_100", "protein_g_per_100", "carbs_g_per_100", "fat_g_per_100",
        "fiber_g_per_100", "salt_g_per_100", "nutrition_json", "url", "image_url", "last_seen"
    )
    values = [product.get(k) for k in fields]
    cur.execute(
        f"""
        INSERT INTO products ({','.join(fields)})
        VALUES ({','.join(['?']*len(fields))})
        ON CONFLICT(ah_id) DO UPDATE SET
            name=excluded.name,
            brand=excluded.brand,
            category=excluded.category,
            unit=excluded.unit,
            price_eur=excluded.price_eur,
            kcal_per_100=excluded.kcal_per_100,
            protein_g_per_100=excluded.protein_g_per_100,
            carbs_g_per_100=excluded.carbs_g_per_100,
            fat_g_per_100=excluded.fat_g_per_100,
            fiber_g_per_100=excluded.fiber_g_per_100,
            salt_g_per_100=excluded.salt_g_per_100,
            nutrition_json=excluded.nutrition_json,
            url=excluded.url,
            image_url=excluded.image_url,
            last_seen=excluded.last_seen
        """,
        values,
    )
    return cur.lastrowid or cur.execute("SELECT id FROM products WHERE ah_id=?", (product.get("ah_id"),)).fetchone()[0]


def insert_recipe(conn: sqlite3.Connection, recipe: Dict, ingredients: Iterable[Dict]) -> int:
    cur = conn.cursor()
    fields = (
        "source", "source_id", "title", "url", "image_url", "servings",
        "total_time_min", "kcal_per_serving", "protein_g_per_serving", "carbs_g_per_serving",
        "fat_g_per_serving", "fiber_g_per_serving", "instructions", "raw_json", "last_seen"
    )
    values = [recipe.get(k) for k in fields]
    cur.execute(
        f"INSERT INTO recipes ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})",
        values,
    )
    recipe_id = cur.lastrowid
    for ing in ingredients:
        cur.execute(
            """
            INSERT INTO ingredients (recipe_id, name, quantity, unit, product_id, raw)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                recipe_id,
                ing.get("name"),
                ing.get("quantity"),
                ing.get("unit"),
                ing.get("product_id"),
                ing.get("raw"),
            ),
        )
    return recipe_id


def insert_recipe_tags(conn: sqlite3.Connection, recipe_id: int, tags: Iterable[Dict]) -> None:
    cur = conn.cursor()
    for t in tags:
        tag = (t.get("tag") or "").strip()
        if not tag:
            continue
        tag_type = t.get("type")
        cur.execute(
            "INSERT INTO recipe_tags (recipe_id, tag, tag_type) VALUES (?, ?, ?)",
            (recipe_id, tag, tag_type),
        )


def get_recipes(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM recipes").fetchall()


def get_products(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT * FROM products").fetchall()


def save_meal_plan(
    conn: sqlite3.Connection,
    date: str,
    target_calories: float,
    meals_per_day: int,
    totals: Dict[str, float],
    items: List[Dict],
    macro_targets: Optional[Dict[str, Optional[float]]] = None,
    slots: Optional[List[str]] = None,
) -> int:
    target_block = {"calories": target_calories}
    if macro_targets:
        # Only include keys that have values
        for k in ("protein_g", "carbs_g", "fat_g"):
            v = macro_targets.get(k)
            if v is not None:
                try:
                    target_block[k] = float(v)
                except Exception:
                    pass
    macros_obj = {
        "target": target_block,
        "actual": totals,
    }
    if slots:
        try:
            macros_obj["slots"] = list(slots)
        except Exception:
            pass
    macros_json = json.dumps(macros_obj)

    attempts = 8
    delay_s = 0.2
    last_err: Optional[Exception] = None
    for _ in range(attempts):
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO meal_plans (
                    date, target_calories, meals_per_day, macros_json, total_calories, total_protein, total_carbs, total_fat
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    target_calories=excluded.target_calories,
                    meals_per_day=excluded.meals_per_day,
                    macros_json=excluded.macros_json,
                    total_calories=excluded.total_calories,
                    total_protein=excluded.total_protein,
                    total_carbs=excluded.total_carbs,
                    total_fat=excluded.total_fat
                """,
                (
                    date,
                    target_calories,
                    meals_per_day,
                    macros_json,
                    totals.get("calories", 0.0),
                    totals.get("protein_g", 0.0),
                    totals.get("carbs_g", 0.0),
                    totals.get("fat_g", 0.0),
                ),
            )
            plan_id = cur.lastrowid or conn.execute("SELECT id FROM meal_plans WHERE date=?", (date,)).fetchone()[0]
            # Clear and insert items
            cur.execute("DELETE FROM meal_plan_items WHERE meal_plan_id=?", (plan_id,))
            for idx, item in enumerate(items):
                cur.execute(
                    """
                    INSERT INTO meal_plan_items (meal_plan_id, meal_index, item_type, item_id, servings, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        plan_id,
                        idx,
                        item["item_type"],
                        item["item_id"],
                        item.get("servings", 1.0),
                        item.get("notes"),
                    ),
                )
            return plan_id
        except sqlite3.OperationalError as e:
            last_err = e
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                time.sleep(delay_s)
                delay_s = min(1.5, delay_s * 1.6)
                continue
            raise
    if last_err:
        raise last_err
    raise sqlite3.OperationalError("Failed to save meal plan")


def mark_seen_page(conn: sqlite3.Connection, url: str, ptype: str, status: str, http_status: Optional[int] = None, last_error: Optional[str] = None):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO seen_pages (url, type, status, http_status, last_error, last_seen)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(url) DO UPDATE SET
            type=excluded.type,
            status=excluded.status,
            http_status=excluded.http_status,
            last_error=excluded.last_error,
            last_seen=excluded.last_seen
        """,
        (url, ptype, status, http_status, last_error),
    )


def is_seen(conn: sqlite3.Connection, url: str) -> bool:
    row = conn.execute("SELECT 1 FROM seen_pages WHERE url=?", (url,)).fetchone()
    return bool(row)
