"""
Tests for reading configuration from the environment.

Run with `make test`.
"""

import logging
import os
import unittest
import unittest.mock

import convalesce_emit.config as ceconfig
import convalesce_emit.errors as ceerrors

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_config1
# #############################################################################


class Test_config1(unittest.TestCase):
    """
    Test that a configuration refuses to send when it cannot.
    """

    def test1(self) -> None:
        """
        Test that sending without an ingest key is refused.

        Failing here rather than at the first request means an operator
        finds out when Airflow starts, not when something breaks at 3am.
        """
        with self.assertRaises(ceerrors.ConfigError):
            ceconfig.Config(ingest_key=None).validate()

    def test2(self) -> None:
        """
        Test that dry-run and disabled need no key, since neither sends.
        """
        ceconfig.Config(ingest_key=None, dry_run=True).validate()
        ceconfig.Config(ingest_key=None, enabled=False).validate()

    def test3(self) -> None:
        """
        Test that a non-http endpoint is refused.
        """
        with self.assertRaises(ceerrors.ConfigError):
            ceconfig.Config(ingest_key="k", endpoint="ftp://nope").validate()


# #############################################################################
# Test_config_from_env1
# #############################################################################


class Test_config_from_env1(unittest.TestCase):
    """
    Test the path customers actually use: variables on the worker, no code.
    """

    def test1(self) -> None:
        """
        Test that values are read and trimmed.
        """
        env = {
            "CONVALESCE_INGEST_KEY": "  from-env  ",
            "CONVALESCE_DRY_RUN": "TRUE",
            "CONVALESCE_BATCH_SIZE": "7",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            config = ceconfig.Config.from_env()
        self.assertEqual(config.ingest_key, "from-env")
        self.assertTrue(config.dry_run)
        self.assertEqual(config.batch_size, 7)

    def test2(self) -> None:
        """
        Test that a non-numeric setting is reported, not silently defaulted.
        """
        env = {"CONVALESCE_TIMEOUT": "soon"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ceerrors.ConfigError):
                ceconfig.Config.from_env()

    def test3(self) -> None:
        """
        Test that an explicit override beats the environment.
        """
        env = {"CONVALESCE_ENDPOINT": "https://from-env.example"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            config = ceconfig.Config.from_env(
                endpoint="https://explicit.example"
            )
        self.assertEqual(config.endpoint, "https://explicit.example")
