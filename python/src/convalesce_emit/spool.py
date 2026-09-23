"""
Local disk for batches that could not be delivered.

A batch the receiver could not take -- it was down, the key was wrong, the
network was out -- is written here instead of being dropped, and sent again
the next time a send succeeds, from this process or any other on the host. A
batch the receiver understood and refused (400, 413, 422) goes to a separate
`rejected` directory: sending it again would be refused again, but it is kept
for whoever needs to see what was in it.

Every file is one gzip-compressed request body, exactly as it would have gone
on the wire, so replaying one is a single POST.

Import as:

import convalesce_emit.spool as cespool
"""

import logging
import os
import time
import uuid
from typing import List, Optional

_LOG = logging.getLogger(__name__)

PENDING = "pending"
REJECTED = "rejected"
_SUFFIX = ".json.gz"
_CLAIMED = ".sending"
# A claim older than this belongs to a process that died mid-send; the batch
# goes back in the queue rather than sitting claimed forever.
_STALE_CLAIM_SECONDS = 600


class Spool:
    """
    Undelivered batches on local disk, oldest first.

    :param directory: where the `pending` and `rejected` directories live
    :param max_bytes: total size `pending` may reach; past it a new batch is
        refused with an error in the log, because filling a customer's disk
        would do more harm than the batch is worth
    """

    def __init__(self, directory: str, max_bytes: int) -> None:
        self.directory = directory
        self.max_bytes = max_bytes

    def save(self, body: bytes, kind: str = PENDING) -> Optional[str]:
        """
        Write one compressed request body.

        :param body: the gzip-compressed body
        :param kind: `PENDING` to send again, `REJECTED` to keep only
        :return: the file written, or None when it could not be
        """
        folder = os.path.join(self.directory, kind)
        try:
            os.makedirs(folder, exist_ok=True)
            if (
                kind == PENDING
                and self._size(folder) + len(body) > self.max_bytes
            ):
                _LOG.error(
                    "convalesce: spool %s is past %d bytes; a batch of %d "
                    "bytes could not be kept",
                    folder,
                    self.max_bytes,
                    len(body),
                )
                return None
            name = f"{time.time_ns():020d}-{uuid.uuid4().hex}{_SUFFIX}"
            path = os.path.join(folder, name)
            partial = path + ".partial"
            with open(partial, "wb") as out:
                out.write(body)
            # Renamed into place so a reader never sees half a file.
            os.replace(partial, path)
            return path
        except OSError as exc:
            _LOG.error(
                "convalesce: could not spool a batch to %s: %s", folder, exc
            )
            return None

    def pending(self) -> List[str]:
        """
        Every batch waiting to be sent again, oldest first.

        Claims left behind by a process that died are released first.

        :return: file paths
        """
        folder = os.path.join(self.directory, PENDING)
        try:
            names = os.listdir(folder)
        except OSError:
            return []
        now = time.time()
        out: List[str] = []
        for name in names:
            path = os.path.join(folder, name)
            if name.endswith(_SUFFIX):
                out.append(path)
            elif _CLAIMED in name:
                try:
                    if now - os.path.getmtime(path) > _STALE_CLAIM_SECONDS:
                        original = path[: path.index(_CLAIMED)]
                        os.replace(path, original)
                        out.append(original)
                except (OSError, ValueError):
                    continue
        return sorted(out)

    def claim(self, path: str) -> Optional[str]:
        """
        Take one batch so no other process sends it at the same time.

        :param path: a path from `pending()`
        :return: the claimed path, or None when another process got it first
        """
        claimed = f"{path}{_CLAIMED}.{os.getpid()}"
        try:
            os.replace(path, claimed)
            os.utime(claimed)
        except OSError:
            return None
        return claimed

    def release(self, claimed: str) -> None:
        """
        Put a claimed batch back, to be tried again later.

        :param claimed: a path from `claim()`
        """
        try:
            os.replace(claimed, claimed[: claimed.index(_CLAIMED)])
        except (OSError, ValueError) as exc:
            _LOG.error(
                "convalesce: could not return %s to the spool: %s", claimed, exc
            )

    def reject(self, claimed: str) -> None:
        """
        Move a claimed batch the receiver refused to `rejected`.

        :param claimed: a path from `claim()`
        """
        folder = os.path.join(self.directory, REJECTED)
        try:
            os.makedirs(folder, exist_ok=True)
            name = os.path.basename(claimed[: claimed.index(_CLAIMED)])
            os.replace(claimed, os.path.join(folder, name))
        except (OSError, ValueError) as exc:
            _LOG.error(
                "convalesce: could not keep refused batch %s: %s", claimed, exc
            )

    @staticmethod
    def read(path: str) -> bytes:
        """
        The compressed body in one file.

        :param path: a claimed path
        :return: its bytes
        """
        with open(path, "rb") as source:
            return source.read()

    @staticmethod
    def done(claimed: str) -> None:
        """
        Remove a batch the receiver accepted.

        :param claimed: a path from `claim()`
        """
        try:
            os.remove(claimed)
        except OSError:
            pass

    @staticmethod
    def _size(folder: str) -> int:
        total = 0
        for name in os.listdir(folder):
            try:
                total += os.path.getsize(os.path.join(folder, name))
            except OSError:
                continue
        return total
