"""MCP server entry point for Claude Code integration."""
import sys
import os

# Add src to path so pbix_mcp can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from pbix_mcp.server import mcp

if __name__ == "__main__":
    mcp.run()
