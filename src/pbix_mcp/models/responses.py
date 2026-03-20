"""
Structured response models for MCP tool boundaries.

Every MCP tool should return a ToolResponse (serialized to JSON string)
so clients can inspect success/failure without string parsing.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ToolResponse(BaseModel):
    """Standard response envelope for all MCP tools."""

    success: bool = True
    error_code: str | None = None
    message: str = ""
    data: Any = None
    warnings: list[str] = Field(default_factory=list)

    def to_text(self) -> str:
        """Render as human-readable text for MCP tool return value."""
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
        """Create a success response."""
        return cls(success=True, message=message, data=data, **kwargs)

    @classmethod
    def error(cls, message: str, code: str = "PBIX_MCP_ERROR", **kwargs) -> ToolResponse:
        """Create an error response."""
        return cls(success=False, error_code=code, message=message, **kwargs)


class DAXResult(BaseModel):
    """Result of a DAX measure evaluation."""

    name: str
    value: Any = None
    status: str = "ok"  # "ok" | "blank" | "unsupported" | "error"
    error_message: str | None = None

    @property
    def is_blank(self) -> bool:
        return self.status == "blank" or (self.status == "ok" and self.value is None)


class DAXEvalResponse(ToolResponse):
    """Response from DAX evaluation tools."""

    results: list[DAXResult] = Field(default_factory=list)

    def to_text(self) -> str:
        if not self.success:
            return super().to_text()

        lines = [f"DAX Evaluation Results ({len(self.results)} measures):\n"]
        for r in self.results:
            if r.status == "error":
                lines.append(f"  {r.name}: ERROR — {r.error_message}")
            elif r.status == "unsupported":
                lines.append(f"  {r.name}: UNSUPPORTED — {r.error_message}")
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


