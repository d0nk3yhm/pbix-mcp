"""MCP server entry point with auto-reload on code changes.

Monitors src/pbix_mcp/ for .py changes. When a file is modified,
all pbix_mcp modules are reloaded before the next tool call.
No Claude Code restart needed.

How it works: Python functions look up global names at CALL TIME,
not definition time. importlib.reload() mutates the module's __dict__
in-place. So the old tool functions (registered in MCP) automatically
see new helper functions, classes, and constants after reload.
"""
import functools
import importlib
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

SRC_DIR = os.path.join(os.path.dirname(__file__), "src", "pbix_mcp")
_last_mtimes: dict[str, float] = {}


def _snapshot() -> dict[str, float]:
    mtimes = {}
    for root, _, files in os.walk(SRC_DIR):
        for f in files:
            if f.endswith(".py"):
                p = os.path.join(root, f)
                try:
                    mtimes[p] = os.path.getmtime(p)
                except OSError:
                    pass
    return mtimes


def _reload_all():
    """Reload every loaded pbix_mcp module (deepest submodules first)."""
    mods = sorted(
        [n for n, m in sys.modules.items()
         if n.startswith("pbix_mcp") and isinstance(m, types.ModuleType)],
        key=lambda n: -n.count("."),
    )
    for name in mods:
        try:
            importlib.reload(sys.modules[name])
        except Exception as e:
            print(f"[pbix-mcp] reload {name}: {e}", file=sys.stderr, flush=True)


def _check_reload():
    """Reload modules if any source file changed since last check."""
    global _last_mtimes
    current = _snapshot()
    if current != _last_mtimes and _last_mtimes:
        changed = [os.path.basename(p) for p in set(current) | set(_last_mtimes)
                    if current.get(p) != _last_mtimes.get(p)]
        print(f"[pbix-mcp] Reloading: {', '.join(changed)}",
              file=sys.stderr, flush=True)
        _reload_all()
    _last_mtimes = current


# Take initial snapshot before importing server
_last_mtimes = _snapshot()

from pbix_mcp.server import mcp  # noqa: E402

# Inject reload check into every tool (sync tools stay sync)
import asyncio
import inspect

for _tool in mcp._tool_manager._tools.values():
    _fn = _tool.fn

    def _wrap(fn):
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                _check_reload()
                return await fn(*args, **kwargs)
            return async_wrapper
        else:
            @functools.wraps(fn)
            def sync_wrapper(*args, **kwargs):
                _check_reload()
                return fn(*args, **kwargs)
            return sync_wrapper

    _tool.fn = _wrap(_fn)

if __name__ == "__main__":
    mcp.run()
