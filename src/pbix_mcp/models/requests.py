"""
Request validation models for MCP tool inputs.

These define the expected shape of tool arguments and provide
validation before execution begins.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FilterContext(BaseModel):
    """Parsed filter context for DAX evaluation."""

    filters: dict[str, list[Any]] = Field(default_factory=dict)

    @classmethod
    def from_json_str(cls, s: str | None) -> FilterContext:
        """Parse from the JSON string that MCP tools receive."""
        if not s:
            return cls()
        import json
        try:
            data = json.loads(s)
            if isinstance(data, dict):
                return cls(filters=data)
        except (json.JSONDecodeError, TypeError):
            pass
        return cls()


class DimensionRef(BaseModel):
    """A Table.Column reference."""

    table: str
    column: str

    @classmethod
    def parse(cls, s: str) -> DimensionRef:
        """Parse 'Table.Column' string."""
        parts = s.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Expected 'Table.Column' format, got: {s!r}")
        return cls(table=parts[0], column=parts[1])
