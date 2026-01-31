from __future__ import annotations

import relevance.domain_models as models
from relevance.infrastructure_repositories import AgencyRepository, SourceRepository


def ensure_agency(repo: AgencyRepository, name: str, aliases: list[str]) -> int:
    existing = repo.get_by_name(name)
    if existing:
        repo.update(existing, aliases)
        return existing.id
    agency = models.Agency(id=None, name=name, aliases=aliases)
    return repo.add(agency).id


def add_source(
    source_repo: SourceRepository,
    agency_id: int,
    source_type: models.SourceType,
    base_url: str,
    config_json: dict,
) -> int:
    source = models.Source(
        id=None,
        agency_id=agency_id,
        source_type=source_type,
        base_url=base_url,
        config_json=config_json,
    )
    existing = source_repo.get_by_base_url(base_url)
    if existing:
        return source_repo.update(existing, source).id
    return source_repo.add(source).id
