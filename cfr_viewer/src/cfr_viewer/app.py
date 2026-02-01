"""Flask application factory."""

import re
from pathlib import Path

from flask import Flask

from ecfr.database import ECFRDatabase
from .routes_browse import browse_bp
from .routes_statistics import statistics_bp
from .routes_compare import compare_bp
from .routes_api import api_bp


def strip_section_prefix(heading: str, section: str) -> str:
    """Remove section prefix (e.g., '§ 7.3   ') from heading if present."""
    if not heading:
        return heading
    # Pattern: § followed by section number and whitespace
    pattern = rf"^§\s*{re.escape(section)}\s+"
    return re.sub(pattern, "", heading)


def create_app(db_path: str | None = None):
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )

    # Create database instance once at startup
    if not db_path:
        db_path = Path(__file__).parent.parent.parent.parent / "ecfr" / "ecfr_data" / "ecfr.db"
    app.ecfr_database = ECFRDatabase(db_path)

    # Register custom Jinja filters
    app.jinja_env.filters["strip_section_prefix"] = strip_section_prefix

    # Register blueprints
    app.register_blueprint(browse_bp)
    app.register_blueprint(statistics_bp, url_prefix="/statistics")
    app.register_blueprint(compare_bp, url_prefix="/compare")
    app.register_blueprint(api_bp, url_prefix="/api")

    return app
