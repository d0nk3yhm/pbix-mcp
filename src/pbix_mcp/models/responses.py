"""
Structured response models for MCP tool boundaries.

Every MCP tool returns a ToolResponse serialized as JSON so clients can
inspect success/failure programmatically without string parsing.
"""

from __future__ import annotations

import datetime as _dt
import math
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


def json_safe_number(value: Any) -> Any:
    """Replace a non-finite float with a JSON-legal token.

    DAX's `/` operator does not blank a divide-by-zero the way DIVIDE() does --
    Desktop returns an IEEE special, so the engine does too (5/0 -> inf,
    0/0 -> nan). Python's json.dumps happily writes those as the bare literals
    `Infinity` and `NaN`, which are NOT valid JSON: a strict client parser
    (JSON.parse, encoding/json, serde_json) rejects the ENTIRE response, so one
    infinite cell would take the whole tool result down. Emit the value as a
    string instead -- lossy in type, but readable and parseable.
    """
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    return value

# Warnings raised deep inside an operation, to be attached to whatever response
# that operation ends up returning.
#
# A rebuild-path edit can discover halfway through that some part of the model
# cannot be carried across — a perspective, a drill-down hierarchy. The code
# that knows this is many frames below the code that builds the response, and
# threading a list through every one of ~40 mutating tools would guarantee that
# some call site is missed. That is the failure mode this whole class of bug
# comes from, so the channel is shared rather than threaded: every response
# drains it, so nothing can be reported as an unqualified success.
_pending_warnings: list[str] = []


def add_pending_warning(message: str) -> None:
    """Attach a warning to the response this operation eventually returns."""
    if message and message not in _pending_warnings:
        _pending_warnings.append(message)


def clear_pending_warnings() -> None:
    """Drop anything buffered — call when starting a fresh operation."""
    _pending_warnings.clear()


def _drain() -> list[str]:
    out = list(_pending_warnings)
    _pending_warnings.clear()
    return out


class ToolResponse(BaseModel):
    """Standard response envelope for all MCP tools."""

    success: bool = True
    error_code: str | None = None
    message: str = ""
    data: Any = None
    warnings: list[str] = Field(default_factory=list)

    def to_text(self) -> str:
        """Serialize as JSON string for MCP tool return value.

        This is the primary serialization method. All MCP tools return
        JSON so clients can parse success/failure programmatically.
        """
        return self.model_dump_json(exclude_none=True)

    def to_human(self) -> str:
        """Render as human-readable text (for logging/debugging)."""
        if not self.success:
            parts = [f"Error [{self.error_code}]: {self.message}"]
            if self.warnings:
                parts.append(f"Warnings: {'; '.join(self.warnings)}")
            return "\n".join(parts)

        parts = []
        if self.message:
            parts.append(self.message)
        if self.data is not None:
            if isinstance(self.data, str):
                parts.append(self.data)
            elif isinstance(self.data, dict):
                for k, v in self.data.items():
                    parts.append(f"  {k}: {v}")
            elif isinstance(self.data, list):
                for item in self.data:
                    parts.append(f"  {item}")
        if self.warnings:
            parts.append(f"\nWarnings: {'; '.join(self.warnings)}")
        return "\n".join(parts)

    @classmethod
    def ok(cls, message: str = "", data: Any = None, **kwargs) -> ToolResponse:
        """Create a success response, carrying any warnings raised en route."""
        kwargs["warnings"] = list(kwargs.get("warnings") or []) + _drain()
        return cls(success=True, message=message, data=data, **kwargs)

    @classmethod
    def error(cls, message: str, code: str = "PBIX_MCP_ERROR", **kwargs) -> ToolResponse:
        """Create an error response."""
        kwargs["warnings"] = list(kwargs.get("warnings") or []) + _drain()
        return cls(success=False, error_code=code, message=message, **kwargs)


class DAXResult(BaseModel):
    """Result of a DAX measure evaluation.

    ``data_type`` states the DAX result type explicitly (issue #24 r22#1: a
    datetime serialized as an ISO string was indistinguishable from a text
    measure, and before the engine fix a with-time datetime came back as its
    bare OLE serial, indistinguishable from a number). Datetimes always
    serialize as ISO-8601 strings with ``data_type: "DateTime"``.
    """

    name: str
    value: Any = None
    status: str = "ok"  # "ok" | "blank" | "unsupported" | "error"
    error_message: str | None = None
    data_type: str | None = None  # "Double"|"String"|"DateTime"|"Boolean"

    @field_validator("value")
    @classmethod
    def _finite_value(cls, v: Any) -> Any:
        return json_safe_number(v)

    @model_validator(mode="after")
    def _derive_data_type(self) -> "DAXResult":
        if self.data_type is None and self.value is not None:
            v = self.value
            if isinstance(v, bool):
                self.data_type = "Boolean"
            elif isinstance(v, (_dt.datetime, _dt.date)):
                self.data_type = "DateTime"
            elif isinstance(v, (int, float)):
                self.data_type = "Double"
            elif isinstance(v, str):
                # json_safe_number stringifies IEEE specials; keep them typed
                # as the numbers they are.
                self.data_type = ("Double" if v in ("NaN", "Infinity",
                                                    "-Infinity") else "String")
        return self

    @property
    def is_blank(self) -> bool:
        return self.status == "blank" or (self.status == "ok" and self.value is None)


class DAXEvalResponse(ToolResponse):
    """Response from DAX evaluation tools."""

    results: list[DAXResult] = Field(default_factory=list)

    def to_text(self) -> str:
        """Serialize as JSON string including results."""
        return self.model_dump_json(exclude_none=True)

    def to_human(self) -> str:
        """Human-readable format for debugging."""
        if not self.success:
            return f"Error [{self.error_code}]: {self.message}"

        lines = [f"DAX Evaluation Results ({len(self.results)} measures):\n"]
        for r in self.results:
            if r.status == "error":
                lines.append(f"  {r.name}: ERROR -- {r.error_message}")
            elif r.status == "unsupported":
                lines.append(f"  {r.name}: UNSUPPORTED -- {r.error_message}")
            elif r.value is None:
                lines.append(f"  {r.name}: (blank)")
            elif isinstance(r.value, float):
                if 0.001 < abs(r.value) < 2:
                    lines.append(f"  {r.name}: {r.value:.1%}")
                elif abs(r.value) >= 1000:
                    lines.append(f"  {r.name}: ${r.value:,.2f}")
                else:
                    lines.append(f"  {r.name}: {r.value:.4f}")
            elif isinstance(r.value, int):
                lines.append(f"  {r.name}: {r.value:,}")
            else:
                lines.append(f"  {r.name}: {r.value}")

        if self.warnings:
            lines.append(f"\nWarnings: {'; '.join(self.warnings)}")
        return "\n".join(lines)
