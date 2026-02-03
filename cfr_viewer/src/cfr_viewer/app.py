"""Flask application factory."""

import re
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template
from ecfr.database import ECFRDatabase
from .routes_browse import browse_bp
from .routes_statistics import statistics_bp
from .routes_compare import compare_bp
from .routes_chart import chart_bp
from .routes_api import api_bp
from .services import BASELINE_YEAR


def create_app(db_path: str | None = None):
    app = Flask(__name__, template_folder=str(Path(__file__).parent / "templates"), static_folder=str(Path(__file__).parent / "static"))
    app.ecfr_database = ECFRDatabase(db_path or Path(__file__).parent.parent.parent.parent / "ecfr" / "ecfr_data" / "ecfr.db")
    app.jinja_env.filters["strip_section_prefix"] = lambda h, s: re.sub(rf"^§\s*{re.escape(s)}\s+", "", h) if h else h
    app.jinja_env.globals["BASELINE_YEAR"] = BASELINE_YEAR
    app.jinja_env.globals["now"] = datetime.now
    app.register_blueprint(browse_bp)
    app.register_blueprint(statistics_bp, url_prefix="/statistics")
    app.register_blueprint(compare_bp, url_prefix="/compare")
    app.register_blueprint(chart_bp, url_prefix="/chart")
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template("errors/404.html"), 404

    return app
