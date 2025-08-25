import os
from flask import Flask


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("AH_MEALPLANNER_SECRET", "dev-secret")

    # Register routes
    from .routes import bp as core_bp
    app.register_blueprint(core_bp)

    return app
