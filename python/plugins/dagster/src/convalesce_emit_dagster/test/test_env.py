"""
Test that this plugin's settings are read under either of their names.

Import as:

import convalesce_emit_dagster.test.test_env as cedtenv
"""

import os
import unittest
import unittest.mock

import convalesce_emit_dagster._env as cedagenv
import convalesce_emit_dagster.steps as cedsteps


class Test_read1(unittest.TestCase):
    """
    Test the one reader every setting of this plugin goes through.
    """

    def test1(self) -> None:
        """
        Test that a setting is read under the prefix a platform puts on it.
        """
        env = {"CUSTOMER_CONVALESCE_SEND_ARGUMENTS": "false"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(cedagenv.read("CONVALESCE_SEND_ARGUMENTS"), "false")
            self.assertFalse(cedsteps.arguments_enabled())

    def test2(self) -> None:
        """
        Test that a setting's own name wins over the prefixed one.
        """
        env = {
            "CONVALESCE_SEND_ARGUMENTS": "true",
            "CUSTOMER_CONVALESCE_SEND_ARGUMENTS": "false",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(cedagenv.read("CONVALESCE_SEND_ARGUMENTS"), "true")
            self.assertTrue(cedsteps.arguments_enabled())

    def test3(self) -> None:
        """
        Test that a setting under neither name is unset.
        """
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(cedagenv.read("CONVALESCE_SEND_ARGUMENTS"))
