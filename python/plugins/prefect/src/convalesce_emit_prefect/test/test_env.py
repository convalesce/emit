"""
Test that this plugin's settings are read under either of their names.

Import as:

import convalesce_emit_prefect.test.test_env as ceptenv
"""

import os
import unittest
import unittest.mock

import convalesce_emit_prefect._capture as cecap
import convalesce_emit_prefect._env as ceprefenv


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
            self.assertEqual(
                ceprefenv.read("CONVALESCE_SEND_ARGUMENTS"), "false"
            )
            self.assertFalse(cecap.send_arguments())

    def test2(self) -> None:
        """
        Test that a setting's own name wins over the prefixed one.
        """
        env = {
            "CONVALESCE_SEND_ARGUMENTS": "true",
            "CUSTOMER_CONVALESCE_SEND_ARGUMENTS": "false",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(ceprefenv.read("CONVALESCE_SEND_ARGUMENTS"), "true")
            self.assertTrue(cecap.send_arguments())

    def test3(self) -> None:
        """
        Test that a setting under neither name is unset.
        """
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(ceprefenv.read("CONVALESCE_SEND_ARGUMENTS"))
