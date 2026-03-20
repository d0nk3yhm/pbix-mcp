"""CLI entry point for pbix-mcp-server."""



def main():
    """Start the MCP server via stdio transport."""
    from pbix_mcp.server import mcp
    mcp.run()


if __name__ == "__main__":
    main()
