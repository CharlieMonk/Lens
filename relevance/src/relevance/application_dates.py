from __future__ import annotations

from datetime import datetime, timezone

from dateutil import parser


def parse_date(value: str) -> datetime:
    parsed = parser.parse(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
