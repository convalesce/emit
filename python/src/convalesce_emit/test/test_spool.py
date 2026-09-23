"""
Tests for keeping undelivered batches on disk.
"""

import logging
import os
import tempfile
import time
import unittest

import convalesce_emit.spool as cespool

_LOG = logging.getLogger(__name__)


class Test_spool1(unittest.TestCase):
    """
    Test saving, claiming and returning batches.
    """

    def setUp(self) -> None:
        # pylint: disable-next=consider-using-with
        self._dir = tempfile.TemporaryDirectory()
        self.spool = cespool.Spool(self._dir.name, max_bytes=1000)

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test1(self) -> None:
        """
        Test that batches come back oldest first, and a claimed one is
        invisible to anyone else until it is released.
        """
        first = self.spool.save(b"one")
        second = self.spool.save(b"two")
        self.assertEqual(self.spool.pending(), [first, second])
        claimed = self.spool.claim(first or "")
        self.assertIsNotNone(claimed)
        self.assertEqual(self.spool.pending(), [second])
        self.assertIsNone(self.spool.claim(first or ""))
        self.spool.release(claimed or "")
        self.assertEqual(self.spool.pending(), [first, second])

    def test2(self) -> None:
        """
        Test that a claim left by a process that died goes back in the queue.
        """
        path = self.spool.save(b"one") or ""
        claimed = self.spool.claim(path) or ""
        old = time.time() - 3600
        os.utime(claimed, (old, old))
        self.assertEqual(self.spool.pending(), [path])

    def test3(self) -> None:
        """
        Test that a full spool refuses a new batch instead of filling the disk.
        """
        self.assertIsNotNone(self.spool.save(b"x" * 900))
        self.assertIsNone(self.spool.save(b"y" * 200))

    def test4(self) -> None:
        """
        Test that a refused batch moves to `rejected` and stays there.
        """
        path = self.spool.save(b"bad") or ""
        self.spool.reject(self.spool.claim(path) or "")
        self.assertEqual(self.spool.pending(), [])
        rejected = os.listdir(os.path.join(self._dir.name, cespool.REJECTED))
        self.assertEqual(rejected, [os.path.basename(path)])
