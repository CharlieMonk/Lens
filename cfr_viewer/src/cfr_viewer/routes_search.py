"""Search routes for CFR content search."""
from flask import Blueprint, render_template, request
from .services import get_database, get_validated_year

search_bp = Blueprint("search", __name__)


@search_bp.route("/")
def index():
    """Search page with FAISS-powered full-text search."""
    db = get_database()
    query = request.args.get("q", "").strip()
    year = get_validated_year()

    # Check if FAISS index is available
    has_index = db.has_similarity_index(year=0)

    results = []
    titles = {}
    if query and has_index:
        results = db.search_by_query(query, year=0, limit=50)
        # Get title names for display
        if results:
            titles = db.get_titles()

    return render_template(
        "search/index.html",
        query=query,
        results=results,
        titles=titles,
        has_index=has_index,
        year=year,
        years=db.list_years(),
    )
