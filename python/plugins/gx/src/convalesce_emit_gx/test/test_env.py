"""
Test that this plugin's settings are read under either of their names.

Import as:

import convalesce_emit_gx.test.test_env as cegtenv
"""

import os
import unittest
import unittest.mock

import convalesce_emit_gx._common as cegxcom


class Test_read1(unittest.TestCase):
    """
    Test the one reader every setting of this plugin goes through.
    """

    def test1(self) -> None:
        """
        Test that a setting is read under the prefix a platform puts on it.
        """
        env = {"CUSTOMER_CONVALESCE_SEND_SOURCE": "false"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                cegxcom.read_setting("CONVALESCE_SEND_SOURCE"), "false"
            )
            self.assertFalse(cegxcom.send_query())

    def test2(self) -> None:
        """
        Test that a setting's own name wins over the prefixed one.
        """
        env = {
            "CONVALESCE_SEND_SOURCE": "true",
            "CUSTOMER_CONVALESCE_SEND_SOURCE": "false",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                cegxcom.read_setting("CONVALESCE_SEND_SOURCE"), "true"
            )
            self.assertTrue(cegxcom.send_query())

    def test3(self) -> None:
        """
        Test that a setting under neither name is unset.
        """
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(cegxcom.read_setting("CONVALESCE_SEND_SOURCE"))
