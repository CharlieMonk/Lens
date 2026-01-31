from __future__ import annotations

from abc import ABC, abstractmethod


class Fetcher(ABC):
    @abstractmethod
    def get(self, url: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_bytes(self, url: str) -> bytes:
        raise NotImplementedError
