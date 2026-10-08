"""
Test that this plugin's settings are read under either of their names.

Import as:

import convalesce_emit_airflow.test.test_env as ceatenv
"""

import os
import unittest
import unittest.mock

import convalesce_emit_airflow._env as cealenv
import convalesce_emit_airflow.carried as cealcar


class Test_read1(unittest.TestCase):
    """
    Test the one reader every setting of this plugin goes through.
    """

    def test1(self) -> None:
        """
        Test that a setting is read under the prefix a platform puts on it.
        """
        env = {"CUSTOMER_CONVALESCE_OPENLINEAGE": "false"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(cealenv.read("CONVALESCE_OPENLINEAGE"), "false")
            self.assertIsNone(cealcar.prepare("2.10.4"))

    def test2(self) -> None:
        """
        Test that a setting's own name wins over the prefixed one.
        """
        env = {
            "CONVALESCE_OPENLINEAGE": "true",
            "CUSTOMER_CONVALESCE_OPENLINEAGE": "false",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(cealenv.read("CONVALESCE_OPENLINEAGE"), "true")

    def test3(self) -> None:
        """
        Test that a setting under neither name is unset.
        """
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(cealenv.read("CONVALESCE_OPENLINEAGE"))
