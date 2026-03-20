"""CLI entry point for pbix-mcp-server."""

import argparse
import os


def main():
    """Start the MCP server via stdio transport."""
    parser = argparse.ArgumentParser(
        prog="pbix-mcp-server",
        description="MCP server for Power BI .pbix/.pbit files",
    )
    parser.add_argument(
        "--log-level",
        choices=["normal", "debug", "trace"],
        default=None,
        help="Logging verbosity (default: normal, or PBIX_MCP_LOG_LEVEL env var)",
    )
    args = parser.parse_args()

    # Set log level before importing server (which imports logging_config)
    if args.log_level:
        os.environ["PBIX_MCP_LOG_LEVEL"] = args.log_level

    from pbix_mcp.logging_config import set_level
    level = args.log_level or os.environ.get("PBIX_MCP_LOG_LEVEL", "normal")
    set_level(level)

    from pbix_mcp.server import mcp
    mcp.run()


if __name__ == "__main__":
    main()
