"""#364: the Claude Code plugin in plugins/pbix-mcp, with this repository as its
marketplace.

The manifests are read by Claude Code and by Anthropic's plugin directory, not
by anything in this package, so nothing else notices when they go wrong. These
tests pin what would break an install without failing any other test:

- the launcher is the one form the directory can lock: uvx with only --python
  and --from. Anything else (uv run, --with-requirements) reports "Launcher lock
  invalid" and uv run adds policy holds, measured on a branch (#365);
- the lock holds the exact PyPI release plugin.json names, and never one newer
  than this package: a plugin released ahead of PyPI cannot start for anyone who
  updates;
- every locked download has a hash, and comes from PyPI;
- it names one Python, and one CI tests. Left open, uvx takes what it finds: 3.14
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

That the lock is current for pyproject.toml is `uv lock --check`, which needs uv
and the network, so it is not run here.

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


@pytest.fixture
def plugin_pyproject() -> str:
    return (PLUGIN / "pyproject.toml").read_text(encoding="utf-8")


@pytest.fixture
def lock_packages() -> list:
    # (read as text: tomllib is not in Python 3.10, which CI runs)
    return (PLUGIN / "uv.lock").read_text(encoding="utf-8").split("[[package]]")[1:]


def _locked(lock_packages: list, name: str) -> str:
    return next(p for p in lock_packages if p.lstrip().startswith(f'name = "{name}"\n'))


def test_the_launcher_is_the_form_the_directory_can_lock(args, manifest):
    assert args == ["--python", _option(args, "--python"), "--from", f"pbix-mcp=={manifest['version']}", "pbix-mcp-server"]


def test_the_lock_holds_the_exact_release_plugin_json_names(lock_packages, plugin_pyproject, manifest):
    assert f'"pbix-mcp=={manifest["version"]}"' in plugin_pyproject
    assert f'\nversion = "{manifest["version"]}"\n' in plugin_pyproject
    assert f'version = "{manifest["version"]}"' in _locked(lock_packages, "pbix-mcp")


def test_every_locked_download_has_a_hash_and_comes_from_pypi(lock_packages):
    assert len(lock_packages) > 1
    for package in lock_packages:
        if 'source = { virtual = "." }' in package:
            continue
        assert 'source = { registry = "https://pypi.org/simple" }' in package
        files = [line for line in package.splitlines() if "url = " in line and "source" not in line]
        assert files
        assert all("files.pythonhosted.org" in line and "hash = \"sha256:" in line for line in files)


def test_the_plugin_is_never_ahead_of_this_package(manifest):
    # the release commit may leave it behind until PyPI has the new version
    assert re.fullmatch(r"\d+\.\d+\.\d+", manifest["version"])
    assert _version(manifest["version"]) <= _version(pbix_mcp.__version__)


def test_the_plugin_names_a_python_ci_tests(args, plugin_pyproject):
    python = _option(args, "--python")
    assert python in CI_PYTHONS
    assert f'requires-python = "=={python}.*"' in plugin_pyproject


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
    assert icon.read_bytes()[1:4] == b"PNG"


def test_the_plugin_folder_has_a_readme_the_directory_accepts():
    assert len((PLUGIN / "README.md").read_text(encoding="utf-8").split()) >= 40


def test_only_the_manifest_is_in_the_plugins_claude_plugin_folder():
    assert [p.name for p in (PLUGIN / ".claude-plugin").iterdir()] == ["plugin.json"]
