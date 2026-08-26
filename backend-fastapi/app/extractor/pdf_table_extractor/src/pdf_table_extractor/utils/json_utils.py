"""Helpers to serialize dataclasses (and pdfplumber's Decimal-based
coordinates) to UTF-8 JSON without losing information."""
from __future__ import annotations

import dataclasses
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


def _default(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (set, tuple)):
        return list(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def to_json(data: Any, indent: int = 2) -> str:
    """Serialize `data` (dataclasses, dicts, lists, ...) to a UTF-8 JSON string."""
    return json.dumps(data, indent=indent, ensure_ascii=False, default=_default)


def write_json(path: Path, data: Any, indent: int = 2) -> None:
    """Serialize `data` and write it to `path`, creating parent folders as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(to_json(data, indent=indent), encoding="utf-8")
