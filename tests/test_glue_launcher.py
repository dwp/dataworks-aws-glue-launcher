#!/usr/bin/env python3

"""batch_job_handler_lambda"""
import pytest
import json
import argparse
from glue_launcher_lambda import glue_launcher

import unittest
from unittest import mock

args = argparse.Namespace()
args.log_level = "INFO"
args.application = "glue_launcher"
args.environment = "development"

class TestRetriever(unittest.TestCase):
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    def test_handler_logs_launching_event(
            self,
            setup_logging_mock,
            get_parameters_mock
    ):
        get_parameters_mock.return_value = args

        event = {"key": "value"}
        glue_launcher.handler(event, None)

        setup_logging_mock.assert_called_once()

if __name__ == "__main__":
    unittest.main()
