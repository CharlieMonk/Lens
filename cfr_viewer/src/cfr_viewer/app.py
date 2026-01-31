"""Flask application factory."""

from pathlib import Path

from flask import Flask

from .routes_browse import browse_bp
from .routes_rankings import rankings_bp
from .routes_compare import compare_bp
from .routes_api import api_bp


def create_app(db_path: str | None = None):
    """Create and configure the Flask application."""
    app = Flask(
        __name__,
        template_folder=str(Path(__file__).parent / "templates"),
        static_folder=str(Path(__file__).parent / "static"),
    )

    # Store db_path in config for services to use
    if db_path:
        app.config["ECFR_DB_PATH"] = db_path
    else:
        # Default to the standard location
        default_path = Path(__file__).parent.parent.parent.parent / "ecfr" / "ecfr_data" / "ecfr.db"
        app.config["ECFR_DB_PATH"] = str(default_path)

    # Register blueprints
    app.register_blueprint(browse_bp)
    app.register_blueprint(rankings_bp, url_prefix="/rankings")
    app.register_blueprint(compare_bp, url_prefix="/compare")
    app.register_blueprint(api_bp, url_prefix="/api")

    return app
