"""CFR Web Viewer - Flask application for browsing CFR data."""

import os
import subprocess
import sys
from .app import create_app

def main():
    """Entry point for cfr-viewer command."""
    # Run fetcher as subprocess so nice 19 doesn't affect web server
    subprocess.run([sys.executable, "-m", "ecfr.fetcher"])
    app = create_app()

    # Warm structure cache if requested (CFR_WARM_CACHE=1 for all, =current for year 0 only)
    warm_cache = os.environ.get("CFR_WARM_CACHE", "").lower()
    if warm_cache in ("1", "true", "all", "current"):
        print("Warming structure cache...", flush=True)
        years = [0] if warm_cache == "current" else None
        count = app.ecfr_database.warm_structure_cache(years=years)
        print(f"  Cached {count} structures", flush=True)

    app.run(debug=True, host="0.0.0.0", port=5000)


__all__ = ["create_app", "main"]
