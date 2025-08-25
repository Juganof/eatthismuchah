AH Personal Meal Planner

Personal-use meal planner that builds daily menus from Dutch recipes (Allerhande and others) and local products (Albert Heijn). It stores data in a local SQLite DB and provides a simple CLI to ingest and plan meals.

Features
- Local SQLite database (no cloud) for personal use
- Ingest recipes from recipe URLs (parses JSON-LD from pages like Allerhande)
- Import Albert Heijn products from a JSON/CSV export you control
- Generate daily meal plans targeting calories and macros

Status
- Scrapers are designed for personal use and to be run by you. The Allerhande parser works from a recipe URL with JSON-LD. The AH product scraper is left as a stub because AH site/API may change and can require anti-bot work; for personal use, import products from your own JSON/CSV export.

Quick Start
1) Initialize the DB:
   python -m ah_mealplanner.cli init-db

2) Ingest your own data

   - Import AH products from your JSON/CSV export:
     python -m ah_mealplanner.cli import-products --file /path/to/your/products.json

   - Ingest a recipe from a URL (e.g., Allerhande):
     python -m ah_mealplanner.cli ingest-allerhande --url "https://www.ah.nl/allerhande/recept/..."

3) Generate a meal plan for a day
   python -m ah_mealplanner.cli plan-day --calories 2200 --meals 3 --exclude "noten,pinda" --date today

Data Model (simplified)
- products: AH products with nutrition per 100g/ml and metadata
- recipes: Recipes with per-serving nutrition and ingredients
- ingredients: Linked to recipes; can optionally map to products
- meal_plans + meal_plan_items: Saved plans by date

Notes on Scraping and Legality
- This code is for your personal use only. Respect website terms, robots.txt, and rate limits. Prefer exporting data you’ve obtained legitimately (e.g., via your own browsing data or manual export) over automated scraping.

Full-Site Crawling (Personal Use)
- Discover + ingest many recipes via sitemaps:
  - python -m ah_mealplanner.cli crawl-allerhande --limit 500
  - Add specific sitemaps with --sitemap URL (repeatable)
- Discover + ingest many AH products via sitemaps:
  - python -m ah_mealplanner.cli crawl-ah-products --limit 500
  - Product parsing is best-effort (JSON-LD / Next.js data); adjust in `ingest_ah.py` if AH changes.

Customize Targets
- You can pass calories and meals per day via CLI. Macro targets default to 30% protein, 35% fat, 35% carbs but can be changed with flags.

Files
- ah_mealplanner/db.py: SQLite connection + schema
- ah_mealplanner/ingest_allerhande.py: Recipe ingestion via JSON-LD
- ah_mealplanner/ingest_ah.py: AH products import + stub scraper
- ah_mealplanner/meal_planner.py: Simple greedy planner
- ah_mealplanner/cli.py: CLI entry point
- ah_mealplanner/web: Minimal Flask web UI (browse, plan)

Web UI (local)
- Install Flask: pip install flask
- Run server:
  - python -m ah_mealplanner.web
- Open http://127.0.0.1:5000
- Pages:
  - /: stats and recent plans
  - /recipes, /recipes/<id>: browse and view recipes
  - /products: browse products
  - /plan: generate a plan and view by date
  - /plan-week: generate plans for a week (or N days) and jump to shopping list
  - /shopping-list?start=YYYY-MM-DD&days=N: aggregated ingredients and products
  - /admin: trigger recipe/product crawls and view recent crawl errors and progress
