"""#364: the Claude Code plugin in plugins/pbix-mcp, with this repository as its
marketplace.

The manifests are read by Claude Code and by Anthropic's plugin directory, not
by anything in this package, so nothing else notices when they go wrong. These
tests pin what would break an install without failing any other test:

- the launcher runs one exact PyPI release (the directory blocks an unpinned
  uvx), the same one plugin.json names, and never one newer than this package:
  a plugin released ahead of PyPI cannot start for anyone who updates;
- it names the Python to use. Without --python, uvx takes what it finds: 3.14
  on a machine with no Python, which CI does not test, and on a machine with a
  damaged uv-managed 3.12 the server failed at startup (No module named
  '_sqlite3');
- the command it runs is one this package installs;
- the marketplace entry names the plugin as plugin.json does (a mismatch fails
  the install: 'Plugin "x" not found in marketplace "y"') and does not declare a
  version of its own (plugin.json wins, and validate reports the difference);
- the directory's blocking rules that a local run can check: a README of at
  least 40 words in the plugin folder, a licence, and no other file in
  .claude-plugin/ (Claude Code does not load one).

Expected values: Claude Code's plugin reference and Anthropic's pre-submission
checklist. That the command really starts and answers was measured over MCP
when this was written (134 tools; a measure evaluated), which needs the network.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import pbix_mcp

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parent.parent
PLUGIN = ROOT / "plugins" / "pbix-mcp"
CI_PYTHONS = ("3.10", "3.11", "3.12", "3.13")


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _version(text: str) -> tuple:
    return tuple(int(part) for part in text.split("."))


@pytest.fixture
def manifest() -> dict:
    return _json(PLUGIN / ".claude-plugin" / "plugin.json")


@pytest.fixture
def marketplace() -> dict:
    return _json(ROOT / ".claude-plugin" / "marketplace.json")


@pytest.fixture
def args() -> list:
    server = _json(PLUGIN / ".mcp.json")["mcpServers"]["powerbi-editor"]
    assert server["command"] == "uvx"
    return server["args"]


def _option(args: list, name: str) -> str:
    return args[args.index(name) + 1]


def test_the_launcher_runs_the_exact_release_plugin_json_names(args, manifest):
    assert _option(args, "--from") == f"pbix-mcp=={manifest['version']}"


def test_the_plugin_is_never_ahead_of_this_package(manifest):
    # the release commit may leave it behind until PyPI has the new version
    assert re.fullmatch(r"\d+\.\d+\.\d+", manifest["version"])
    assert _version(manifest["version"]) <= _version(pbix_mcp.__version__)


def test_the_launcher_names_a_python_ci_tests(args):
    assert _option(args, "--python") in CI_PYTHONS


def test_the_command_is_one_the_package_installs(args):
    # (read as text: tomllib is not in Python 3.10, which CI runs)
    scripts = (ROOT / "pyproject.toml").read_text(encoding="utf-8").split("[project.scripts]")[1]
    assert re.search(rf"^{re.escape(args[-1])}\s*=", scripts, re.M)


def test_the_marketplace_entry_matches_the_plugin(marketplace, manifest):
    entry = next(p for p in marketplace["plugins"] if p["name"] == manifest["name"])
    assert (ROOT / entry["source"] / ".claude-plugin" / "plugin.json").is_file()
    assert "version" not in entry
    assert marketplace["owner"]["name"]


@pytest.mark.parametrize("key", ["description", "version", "license"])
def test_the_manifest_has_what_the_directory_asks_for(manifest, key):
    assert manifest[key]


def test_the_manifest_names_an_author(manifest):
    assert manifest["author"]["name"]


def test_the_manifests_icon_is_a_png_in_the_plugin(manifest):
    # the directory portal's checks look for an icon in plugin.json
    icon = PLUGIN / manifest["icon"]
    assert manifest["icon"].startswith("./")
    assert icon.read_bytes()[:8] == b"PNG

"


def test_the_plugin_folder_has_a_readme_the_directory_accepts():
    assert len((PLUGIN / "README.md").read_text(encoding="utf-8").split()) >= 40


def test_only_the_manifest_is_in_the_plugins_claude_plugin_folder():
    assert [p.name for p in (PLUGIN / ".claude-plugin").iterdir()] == ["plugin.json"]
