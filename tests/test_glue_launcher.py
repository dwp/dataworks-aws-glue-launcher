#!/usr/bin/env python3

"""glue_launcher_lambda"""
import pytest
import json
import argparse
from glue_launcher_lambda import glue_launcher

import unittest
from unittest import mock

JOB_NAME_KEY = "jobName"
JOB_STATUS_KEY = "status"
JOB_QUEUE_KEY = "jobQueue"

FAILED_JOB_STATUS = "FAILED"
PENDING_JOB_STATUS = "PENDING"
RUNNABLE_JOB_STATUS = "RUNNABLE"
STARTING_JOB_STATUS = "STARTING"
SUCCEEDED_JOB_STATUS = "SUCCEEDED"

JOB_NAME = "test job"

args = argparse.Namespace()
args.log_level = "INFO"
args.application = "glue_launcher"
args.environment = "development"
args.job_queue_dependencies = "testqueue, queuetest"


class TestRetriever(unittest.TestCase):
    # @mock.patch(
    #     "glue_launcher_lambda.glue_launcher.get_and_validate_job_details"
    # )
    # @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    # @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    # def test_handler_logs_launching_event(
    #         self,
    #         setup_logging_mock,
    #         get_parameters_mock,
    #         get_and_validate_job_details_mock
    # ):
    #     get_parameters_mock.return_value = args
    #
    #     details_dict = {
    #         JOB_NAME_KEY: JOB_NAME,
    #         JOB_STATUS_KEY: PENDING_JOB_STATUS,
    #         JOB_QUEUE_KEY: JOB_QUEUE_KEY,
    #     }
    #
    #     get_and_validate_job_details_mock.return_value = details_dict
    #
    #     event = {"key": "value"}
    #     glue_launcher.handler(event, None)
    #
    #     setup_logging_mock.assert_called_once()

    @mock.patch(
        "glue_launcher_lambda.glue_launcher.get_and_validate_job_details"
    )
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_handler_ignores_jobs_with_ignored_status(
            self,
            mock_logger,
            get_parameters_mock,
            setup_logging_mock,
            get_and_validate_job_details_mock,
    ):
        get_parameters_mock.return_value = args

        details_dict = {
            JOB_NAME_KEY: JOB_NAME,
            JOB_STATUS_KEY: PENDING_JOB_STATUS,
            JOB_QUEUE_KEY: JOB_QUEUE_KEY,
        }

        get_and_validate_job_details_mock.return_value = details_dict

        event = {
            "test_key": "test_value",
        }

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            glue_launcher.handler(event, None)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 0


    @mock.patch("glue_launcher_lambda.glue_launcher.check_running_batch_tasks")
    @mock.patch(
        "glue_launcher_lambda.glue_launcher.get_and_validate_job_details"
    )
    @mock.patch(
        "glue_launcher_lambda.glue_launcher.get_batch_client"
    )
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_batch_queue_jobs_present_no_further_action(self,
                                                        mock_logger,
                                                        get_parameters_mock,
                                                        setup_logging_mock,
                                                        get_batch_client_mock,
                                                        get_and_validate_job_details_mock,
                                                        running_batch_tasks_mock
                                                        ):
        get_parameters_mock.return_value = args
        running_batch_tasks_mock.return_value = 2

        details_dict = {
            JOB_NAME_KEY: JOB_NAME,
            JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
            JOB_QUEUE_KEY: JOB_QUEUE_KEY,
        }

        get_and_validate_job_details_mock.return_value = details_dict

        event = {
            "test_key": "test_value",
        }

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            glue_launcher.handler(event, None)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 0


if __name__ == "__main__":
    unittest.main()
