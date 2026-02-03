"""CFR Web Viewer - Flask application for browsing CFR data."""

import os
import subprocess
import sys
from .app import create_app
from ecfr.config import config

def main():
    """Entry point for cfr-viewer command."""
    # Run fetcher as subprocess so nice 19 doesn't affect web server
    subprocess.run([sys.executable, "-m", "ecfr.fetcher"])
    app = create_app()

    # Warm structure cache if requested via config or environment variable
    warm_cache = config.flask_warm_cache
    if warm_cache:
        warm_str = str(warm_cache).lower()
        if warm_str in ("1", "true", "all", "current"):
            print("Warming structure cache...", flush=True)
            years = [0] if warm_str == "current" else None
            count = app.ecfr_database.warm_structure_cache(years=years)
            print(f"  Cached {count} structures", flush=True)

    app.run(debug=True, host=config.flask_host, port=config.flask_port)


__all__ = ["create_app", "main"]
