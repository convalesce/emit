"""
Keeps every test's undelivered batches in its own directory.

Without this a test that fails a send leaves a batch in the shared spool,
and the next test to send successfully delivers it to its own server.
"""

import pathlib

import pytest


@pytest.fixture(autouse=True)
def _own_spool(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONVALESCE_SPOOL_DIR", str(tmp_path / "spool"))
