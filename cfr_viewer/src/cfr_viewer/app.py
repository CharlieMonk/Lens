"""Flask application factory."""

import os
import re
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, url_for
from ecfr.database import ECFRDatabase
from .routes_browse import browse_bp
from .routes_agencies import agencies_bp
from .routes_compare import compare_bp
from .routes_chart import chart_bp
from .routes_api import api_bp
from .services import BASELINE_YEAR


def create_app(db_path: str | None = None):
    app = Flask(__name__, template_folder=str(Path(__file__).parent / "templates"), static_folder=str(Path(__file__).parent / "static"))
    app.ecfr_database = ECFRDatabase(db_path)  # Uses config.database_path if db_path is None
    app.jinja_env.filters["strip_section_prefix"] = lambda h, s: re.sub(rf"^§\s*{re.escape(s)}\s+", "", h) if h else h
    app.jinja_env.globals["BASELINE_YEAR"] = BASELINE_YEAR
    app.jinja_env.globals["now"] = datetime.now
    app.register_blueprint(browse_bp)
    app.register_blueprint(agencies_bp, url_prefix="/agencies")
    app.register_blueprint(compare_bp, url_prefix="/compare")
    app.register_blueprint(chart_bp, url_prefix="/chart")
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.errorhandler(404)
    def page_not_found(e):
        return render_template("errors/404.html"), 404

    @app.context_processor
    def override_url_for():
        def versioned_url_for(endpoint, **values):
            if endpoint == "static":
                filename = values.get("filename")
                if filename:
                    path = os.path.join(app.static_folder, filename)
                    if os.path.isfile(path):
                        values["v"] = int(os.path.getmtime(path))
            return url_for(endpoint, **values)
        return dict(url_for=versioned_url_for)

    return app
