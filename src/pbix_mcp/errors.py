"""
Typed exceptions and error codes for pbix-mcp.

Every failure path in the package should land in one of these exceptions
with a stable error code that clients can inspect programmatically.
"""


class PBIXMCPError(Exception):
    """Base exception for all pbix-mcp errors."""

    code: str = "PBIX_MCP_ERROR"

    def __init__(self, message: str, code: str | None = None):
        self.message = message
        if code:
            self.code = code
        super().__init__(f"[{self.code}] {message}")


# ---------------------------------------------------------------------------
# File / format errors
# ---------------------------------------------------------------------------

class InvalidPBIXError(PBIXMCPError):
    """The file is not a valid PBIX/PBIT or is corrupted."""
    code = "PBIX_INVALID"


class UnsupportedFormatError(PBIXMCPError):
    """The file uses a format variant that is not supported."""
    code = "FORMAT_UNSUPPORTED"


class LayoutParseError(PBIXMCPError):
    """The Report/Layout JSON could not be parsed."""
    code = "LAYOUT_JSON_INVALID"


class DataModelCompressionError(PBIXMCPError):
    """XPress9 decompress or recompress failed."""
    code = "DATAMODEL_DECOMPRESS_FAILED"


class ABFRebuildError(PBIXMCPError):
    """ABF archive read, modify, or rebuild failed."""
    code = "ABF_REBUILD_FAILED"


class MetadataSQLError(PBIXMCPError):
    """SQLite metadata read or write failed."""
    code = "METADATA_SQL_FAILED"


# ---------------------------------------------------------------------------
# DAX errors
# ---------------------------------------------------------------------------

class DAXError(PBIXMCPError):
    """Base for all DAX-related errors."""
    code = "DAX_ERROR"


class DAXUnsupportedError(DAXError):
    """A DAX function or pattern is not implemented."""
    code = "DAX_UNSUPPORTED_FUNCTION"

    def __init__(self, function_name: str, message: str | None = None):
        self.function_name = function_name
        msg = message or f"DAX function '{function_name}' is not supported"
        super().__init__(msg, self.code)


class DAXEvaluationError(DAXError):
    """DAX expression evaluation failed at runtime."""
    code = "DAX_EVAL_FAILED"


class DAXParseError(DAXError):
    """DAX expression could not be parsed."""
    code = "DAX_PARSE_FAILED"


# ---------------------------------------------------------------------------
# Write / safety errors
# ---------------------------------------------------------------------------

class UnsafeWriteError(PBIXMCPError):
    """A destructive write was attempted without explicit confirmation."""
    code = "UNSAFE_WRITE"


class SessionError(PBIXMCPError):
    """File session error — alias not found, already open, etc."""
    code = "SESSION_ERROR"


class FileNotOpenError(SessionError):
    """The requested alias is not open."""
    code = "FILE_NOT_OPEN"


class FileAlreadyOpenError(SessionError):
    """The file or alias is already open."""
    code = "FILE_ALREADY_OPEN"


# ---------------------------------------------------------------------------
# Error code registry (for documentation / client parsing)
# ---------------------------------------------------------------------------

ERROR_CODES = {
    "PBIX_MCP_ERROR": "Generic pbix-mcp error",
    "PBIX_INVALID": "File is not a valid PBIX/PBIT",
    "FORMAT_UNSUPPORTED": "Unsupported file format variant",
    "LAYOUT_JSON_INVALID": "Report layout JSON could not be parsed",
    "DATAMODEL_DECOMPRESS_FAILED": "XPress9 DataModel decompression failed",
    "ABF_REBUILD_FAILED": "ABF archive operation failed",
    "METADATA_SQL_FAILED": "SQLite metadata operation failed",
    "DAX_ERROR": "Generic DAX error",
    "DAX_UNSUPPORTED_FUNCTION": "DAX function not implemented",
    "DAX_EVAL_FAILED": "DAX evaluation failed at runtime",
    "DAX_PARSE_FAILED": "DAX expression parse failure",
    "UNSAFE_WRITE": "Destructive write attempted without confirmation",
    "SESSION_ERROR": "File session error",
    "FILE_NOT_OPEN": "Requested file alias is not open",
    "FILE_ALREADY_OPEN": "File or alias is already open",
}
