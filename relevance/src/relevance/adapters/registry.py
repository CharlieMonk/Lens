from __future__ import annotations

from relevance.adapters.base import AgencyAdapter
from relevance.adapters.dol import DolEnforcementAdapter
from relevance.adapters.epa import EpaEnforcementAdapter
from relevance.adapters.sec import SecEnforcementAdapter


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, AgencyAdapter] = {}
        for adapter in [SecEnforcementAdapter(), EpaEnforcementAdapter(), DolEnforcementAdapter()]:
            self._adapters[adapter.agency_name] = adapter

    def get(self, agency_name: str) -> AgencyAdapter:
        if agency_name not in self._adapters:
            raise KeyError(f"No adapter registered for {agency_name}")
        return self._adapters[agency_name]

    def list(self) -> list[AgencyAdapter]:
        return list(self._adapters.values())
