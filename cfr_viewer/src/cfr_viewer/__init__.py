"""CFR Web Viewer - Flask application for browsing CFR data."""

from .app import create_app
import ecfr.fetcher

def main():
    """Entry point for cfr-viewer command."""
    ecfr.fetcher.main()
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5000)


__all__ = ["create_app", "main"]
