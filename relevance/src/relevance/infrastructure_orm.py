from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class AgencyModel(Base):
    __tablename__ = "agencies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    aliases: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    sources = relationship("SourceModel", back_populates="agency")


class SourceModel(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    base_url: Mapped[str] = mapped_column(String(500), nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    agency = relationship("AgencyModel", back_populates="sources")


class DocumentModel(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    url: Mapped[str] = mapped_column(String(1000), nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    retrieved_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (
        UniqueConstraint("url", name="uq_documents_url"),
        Index("ix_documents_url", "url"),
    )


class CitationModel(Base):
    __tablename__ = "citations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title_number: Mapped[int] = mapped_column(Integer, nullable=False)
    part: Mapped[str] = mapped_column(String(50), nullable=False)
    section: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    raw_text: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized: Mapped[str] = mapped_column(String(100), nullable=False)
    citation_type: Mapped[str] = mapped_column(String(20), nullable=False)

    __table_args__ = (Index("ix_citations_normalized", "normalized"),)


class DocumentCitationModel(Base):
    __tablename__ = "document_citations"

    document_id: Mapped[int] = mapped_column(
        ForeignKey("documents.id"), primary_key=True
    )
    citation_id: Mapped[int] = mapped_column(
        ForeignKey("citations.id"), primary_key=True
    )
    context_snippet: Mapped[str] = mapped_column(String(400), nullable=False)
    match_start: Mapped[int] = mapped_column(Integer, nullable=False)
    match_end: Mapped[int] = mapped_column(Integer, nullable=False)
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        Index("ix_doc_citations_doc_id_cit_id", "document_id", "citation_id"),
    )


class CitationCountModel(Base):
    __tablename__ = "citation_counts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False)
    normalized_citation: Mapped[str] = mapped_column(String(100), nullable=False)
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        Index(
            "ix_citation_counts_agency_citation",
            "agency_id",
            "normalized_citation",
        ),
    )


class SchemaMigrationModel(Base):
    __tablename__ = "schema_migrations"

    version: Mapped[str] = mapped_column(String(50), primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
