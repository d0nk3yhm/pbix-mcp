"""pbix_open must not strand its extraction directories.

Thousands of ``pbix_mcp_*`` directories were found accumulated in the system
temp directory. The sweep/probe/test class of caller exits (or is killed)
without ever calling ``pbix_close``, and nothing else deleted the extraction.

Two independent mechanisms, both covered here:

* an ``atexit`` hook removes every directory THIS process created and has not
  already closed -- covers normal interpreter exit without close;
* a once-per-process scavenger, run on the first ``pbix_open``, deletes sibling
  directories whose owning pid (parsed from the END of the name, because an
  alias may contain underscores) is dead -- covers hard kills, where atexit
  never runs. Live and unparseable names are kept unless a 7-day backstop
  passes, so an ACTIVE extraction is never touched even under pid reuse.
"""
from __future__ import annotations

import os
import tempfile
import time
import uuid

from pbix_mcp import server


def _fake_dir(pid, age_days=0.0):
    name = f"pbix_mcp_t_20260101_000000_{pid}_{uuid.uuid4().hex[:8]}"
    d = os.path.join(tempfile.gettempdir(), name)
    os.makedirs(d, exist_ok=True)
    if age_days:
        past = time.time() - age_days * 86400
        os.utime(d, (past, past))
    return d


class TestAtexitCleanup:
    def test_registered_dirs_are_removed(self, tmp_path):
        d = tmp_path / "wd"
        d.mkdir()
        server._work_dirs.add(str(d))
        server._cleanup_own_work_dirs()
        assert not d.exists()
        assert str(d) not in server._work_dirs

    def test_close_unregisters_so_atexit_does_not_double_delete(self, tmp_path):
        # pbix_close discards the dir from the set; simulate that contract.
        d = tmp_path / "wd2"
        d.mkdir()
        server._work_dirs.add(str(d))
        server._work_dirs.discard(str(d))
        server._cleanup_own_work_dirs()
        assert d.exists()          # no longer ours to delete


class TestScavenger:
    def _run_scavenge(self):
        server._scavenged = False
        server._scavenge_stale_work_dirs()

    def test_dead_pid_is_deleted(self):
        # A pid far above the plausible range on Windows and Linux alike;
        # if it happens to be alive the test is meaningless, so guard.
        dead_pid = 4000000000
        assert not server._pid_alive(dead_pid)
        d = _fake_dir(dead_pid)
        try:
            self._run_scavenge()
            assert not os.path.isdir(d)
        finally:
            if os.path.isdir(d):
                os.rmdir(d)

    def test_live_pid_is_kept(self):
        d = _fake_dir(os.getpid())
        try:
            self._run_scavenge()
            assert os.path.isdir(d)
        finally:
            os.rmdir(d)

    def test_unparseable_young_dir_is_kept(self):
        d = os.path.join(tempfile.gettempdir(), "pbix_mcp_oldformat")
        os.makedirs(d, exist_ok=True)
        try:
            self._run_scavenge()
            assert os.path.isdir(d)
        finally:
            if os.path.isdir(d):
                os.rmdir(d)

    def test_unparseable_dir_past_backstop_is_deleted(self):
        d = os.path.join(tempfile.gettempdir(), "pbix_mcp_ancient")
        os.makedirs(d, exist_ok=True)
        past = time.time() - 8 * 86400
        os.utime(d, (past, past))
        try:
            self._run_scavenge()
            assert not os.path.isdir(d)
        finally:
            if os.path.isdir(d):
                os.rmdir(d)

    def test_own_registered_dir_is_never_scavenged(self):
        """Even with a live-pid name, a dir in _work_dirs belongs to us and the
        scavenger must skip it outright."""
        d = _fake_dir(os.getpid())
        server._work_dirs.add(d)
        try:
            self._run_scavenge()
            assert os.path.isdir(d)
        finally:
            server._work_dirs.discard(d)
            os.rmdir(d)

    def test_pid_alive_on_self(self):
        assert server._pid_alive(os.getpid())
