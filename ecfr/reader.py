"""Interface for querying CFR data from the database."""

from pathlib import Path

from .database import ECFRDatabase


class ECFRReader:
    """Interface for reading and navigating eCFR data from SQLite database.

    This is a convenience wrapper around ECFRDatabase for read-only queries.
    It provides backwards compatibility and a simpler interface for consumers
    who only need to query data, not write it.
    """

    def __init__(self, db_path: str = "ecfr/ecfr_data/ecfr.db"):
        self._db = ECFRDatabase(Path(db_path))

    # Delegate all read methods to the database

    def list_years(self) -> list[int]:
        """List available years from database (0 = current)."""
        return self._db.list_years()

    def list_titles(self, year: int = 0) -> list[int]:
        """List available title numbers from database."""
        return self._db.list_section_titles(year)

    def navigate(
        self,
        title: int,
        subtitle: str = None,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        section: str = None,
        year: int = 0,
    ) -> dict | None:
        """Navigate to a specific location in the CFR hierarchy."""
        return self._db.navigate(
            title=title,
            subtitle=subtitle,
            chapter=chapter,
            subchapter=subchapter,
            part=part,
            subpart=subpart,
            section=section,
            year=year,
        )

    def search(self, query: str, title: int = None, year: int = 0) -> list[dict]:
        """Full-text search across sections."""
        return self._db.search(query, title=title, year=year)

    def get_structure(self, title: int, year: int = 0) -> dict:
        """Return hierarchy tree for a title."""
        return self._db.get_structure(title, year=year)

    def get_word_counts(
        self,
        title: int,
        chapter: str = None,
        subchapter: str = None,
        part: str = None,
        subpart: str = None,
        year: int = 0,
    ) -> dict:
        """Get word counts for sections."""
        return self._db.get_section_word_counts(
            title=title,
            chapter=chapter,
            subchapter=subchapter,
            part=part,
            subpart=subpart,
            year=year,
        )

    def get_total_words(self, title: int, year: int = 0) -> int:
        """Get total word count for a title."""
        return self._db.get_total_section_words(title, year=year)

    def get_section_heading(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the heading text for a section."""
        return self._db.get_section_heading(title, section, year=year)

    def get_section_text(self, title: int, section: str, year: int = 0) -> str | None:
        """Get the full text content of a section."""
        return self._db.get_section_text(title, section, year=year)

    def get_section(self, title: int, section: str, year: int = 0) -> dict | None:
        """Get full section data."""
        return self._db.get_section(title, section, year=year)

    def get_sections(
        self,
        title: int,
        chapter: str = None,
        part: str = None,
        year: int = 0,
    ) -> list[dict]:
        """Get all sections for a title."""
        return self._db.get_sections(title, chapter=chapter, part=part, year=year)

    def get_similar_sections(
        self,
        title: int,
        section: str,
        year: int = 0,
        limit: int = 10,
        min_similarity: float = 0.1,
    ) -> list[dict]:
        """Find sections similar to a given section based on TF-IDF cosine similarity."""
        return self._db.get_similar_sections(
            title=title,
            section=section,
            year=year,
            limit=limit,
            min_similarity=min_similarity,
        )

    def get_most_similar_pairs(
        self,
        year: int = 0,
        limit: int = 20,
        min_similarity: float = 0.5,
        title: int = None,
    ) -> list[dict]:
        """Get the most similar section pairs across all titles."""
        return self._db.get_most_similar_pairs(
            year=year,
            limit=limit,
            min_similarity=min_similarity,
            title=title,
        )

    def find_duplicate_regulations(
        self,
        year: int = 0,
        min_similarity: float = 0.95,
        limit: int = 100,
    ) -> list[dict]:
        """Find potential duplicate regulations (sections with very high similarity)."""
        return self._db.find_duplicate_regulations(
            year=year,
            min_similarity=min_similarity,
            limit=limit,
        )

    def similarity_stats(self, year: int = 0) -> dict:
        """Get statistics about section similarities in the database."""
        return self._db.similarity_stats(year=year)
