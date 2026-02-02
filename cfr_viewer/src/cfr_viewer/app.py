"""Flask application factory."""

import re
from pathlib import Path
from flask import Flask
from ecfr.database import ECFRDatabase
from .routes_browse import browse_bp
from .routes_statistics import statistics_bp
from .routes_compare import compare_bp
from .routes_api import api_bp


def create_app(db_path: str | None = None):
    app = Flask(__name__, template_folder=str(Path(__file__).parent / "templates"), static_folder=str(Path(__file__).parent / "static"))
    app.ecfr_database = ECFRDatabase(db_path or Path(__file__).parent.parent.parent.parent / "ecfr" / "ecfr_data" / "ecfr.db")
    app.jinja_env.filters["strip_section_prefix"] = lambda h, s: re.sub(rf"^§\s*{re.escape(s)}\s+", "", h) if h else h
    app.register_blueprint(browse_bp)
    app.register_blueprint(statistics_bp, url_prefix="/statistics")
    app.register_blueprint(compare_bp, url_prefix="/compare")
    app.register_blueprint(api_bp, url_prefix="/api")
    return app
