#!/usr/bin/env python3

"""glue_launcher_lambda"""
import pytest
import json
import os
import argparse
import copy
from glue_launcher_lambda import glue_launcher

import unittest
from unittest import mock
from unittest.mock import call, MagicMock

JOB_NAME_KEY = "jobName"
JOB_STATUS_KEY = "status"
JOB_QUEUE_KEY = "jobQueue"

FAILED_JOB_STATUS = "FAILED"
PENDING_JOB_STATUS = "PENDING"
RUNNABLE_JOB_STATUS = "RUNNABLE"
STARTING_JOB_STATUS = "STARTING"
SUCCEEDED_JOB_STATUS = "SUCCEEDED"

JOB_NAME = "test job"

SQL_FILE_LOCATION = "tests/sql"
ETL_GLUE_JOB_NAME = "GLUE_JOB"

MANIFEST_COMPARISON_CUT_OFF_DATE_START = "1983-11-15T09:09:55.000"
MANIFEST_COMPARISON_CUT_OFF_DATE_END = "2099-11-15T09:09:55.000"

args = argparse.Namespace()
args.log_level = "INFO"
args.application = "glue_launcher"
args.environment = "development"
args.job_queue_dependencies = "testqueue, queuetest"

# Fetch table values
args.missing_imports_table_name = "missing_imports"
args.missing_exports_table_name = "missing_exports"
args.counts_table_name = "counts"
args.mismatched_timestamps_table_name = "mismatched_timestamps"
args.manifest_s3_input_parquet_location_missing_import = "/missing_imports"
args.manifest_s3_input_parquet_location_missing_export = "/missing_exports"
args.manifest_s3_input_parquet_location_counts = "/counts"
args.manifest_s3_input_parquet_location_mismatched_timestamps = "/mismatched_timestamps"
args.manifest_s3_output_location = "s3://bucket/output_location"


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

    def test_batch_queue_jobs_empty_fetch_table_creation_sql(self):
        with open(os.path.join(SQL_FILE_LOCATION, "create-parquet-table.sql"), "r") as f:
            base_create_parquet_query = f.read()

        with open(os.path.join(SQL_FILE_LOCATION, "create-missing-import-table.sql"), "r", ) as f:
            base_create_missing_import_query = f.read()

        with open(os.path.join(SQL_FILE_LOCATION, "create-missing-export-table.sql"), "r", ) as f:
            base_create_missing_export_query = f.read()

        with open(os.path.join(SQL_FILE_LOCATION, "create-count-table.sql"), "r") as f:
            base_create_count_query = f.read()

        expected = [
            [args.missing_imports_table_name, base_create_parquet_query,
             args.manifest_s3_input_parquet_location_missing_import],
            [args.missing_exports_table_name, base_create_missing_import_query,
             args.manifest_s3_input_parquet_location_missing_export],
            [args.counts_table_name, base_create_missing_export_query,
             args.manifest_s3_input_parquet_location_counts],
            [args.mismatched_timestamps_table_name, base_create_count_query,
             args.manifest_s3_input_parquet_location_mismatched_timestamps],
        ]
        actual = glue_launcher.fetch_table_creation_sql_files(SQL_FILE_LOCATION, args)
        assert expected == actual, f"Expected does not equal actual. Expected '{expected}' but got '{actual}'"

    @mock.patch(
        "glue_launcher_lambda.glue_launcher.get_and_validate_job_details"
    )
    def test_batch_queue_jobs_empty_fetch_table_drop_sql(self,
                                                         get_and_validate_job_details_mock,
                                                         ):
        with open(os.path.join(SQL_FILE_LOCATION, "drop-table.sql"), "r") as f:
            base_drop_query = f.read()

        expected = base_drop_query
        actual = glue_launcher.fetch_table_drop_sql_file(SQL_FILE_LOCATION, args)

        assert expected == actual, f"Expected does not equal actual. Expected '{expected}' but got '{actual}'"


    @mock.patch("glue_launcher_lambda.glue_launcher.execute_athena_query")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_recreate_single_table(self,
                                   mock_logger,
                                   execute_athena_mock,
                                   ):

        athena_client_mock = mock

        with open(os.path.join(SQL_FILE_LOCATION, "drop-table.sql"), "r") as f:
            base_drop_query = f.read()

        tables = [
            [
                args.missing_imports_table_name,
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS [table_name]
                LOCATION '[s3_input_location]'
                """,
                args.manifest_s3_input_parquet_location_missing_import
            ]
        ]

        drop_query = base_drop_query.replace("[table_name]", args.missing_imports_table_name)
        create_query = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {args.missing_imports_table_name}
                LOCATION '{args.manifest_s3_input_parquet_location_missing_import}/'
                """

        actual = glue_launcher.recreate_sql_tables(tables, base_drop_query, athena_client_mock)

        execute_athena_mock_calls = [
            call(args.manifest_s3_output_location, drop_query, athena_client_mock),
            call(args.manifest_s3_output_location, create_query, athena_client_mock)
        ]

        execute_athena_mock.assert_has_calls(execute_athena_mock_calls)


    @mock.patch("glue_launcher_lambda.glue_launcher.execute_athena_query")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_recreate_multiple_tables(self,
                                   mock_logger,
                                   execute_athena_mock,
                                   ):

        athena_client_mock = mock

        with open(os.path.join(SQL_FILE_LOCATION, "drop-table.sql"), "r") as f:
            base_drop_query = f.read()

        tables = [
            [
                args.missing_imports_table_name,
                "CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] LOCATION '[s3_input_location]'",
                args.manifest_s3_input_parquet_location_missing_import
            ],
            [
                args.missing_exports_table_name,
                "CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] LOCATION '[s3_input_location]'",
                args.manifest_s3_input_parquet_location_missing_export
            ]
        ]

        glue_launcher.recreate_sql_tables(tables, base_drop_query, athena_client_mock)

        missing_imports_drop_query = f"DROP * FROM {args.missing_imports_table_name};\n"
        missing_imports_create_query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {args.missing_imports_table_name} LOCATION '{args.manifest_s3_input_parquet_location_missing_import}/'"

        missing_exports_drop_query = f"DROP * FROM {args.missing_exports_table_name};\n"
        missing_exports_create_query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {args.missing_exports_table_name} LOCATION '{args.manifest_s3_input_parquet_location_missing_export}/'"

        execute_athena_mock_calls = [
            call(args.manifest_s3_output_location, missing_imports_drop_query, athena_client_mock),
            call(args.manifest_s3_output_location, missing_imports_create_query, athena_client_mock),
            call(args.manifest_s3_output_location, missing_exports_drop_query, athena_client_mock),
            call(args.manifest_s3_output_location, missing_exports_create_query, athena_client_mock),
        ]

        execute_athena_mock.assert_has_calls(execute_athena_mock_calls)


    @mock.patch("glue_launcher_lambda.glue_launcher.get_glue_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_execute_manifest_glue_job(self,
                                       mock_logger,
                                       glue_client_mock):

        glue_client_mock.start_job_run = MagicMock()
        glue_client_mock.start_job_run.return_value = "12"

        glue_launcher.execute_manifest_glue_job(
            ETL_GLUE_JOB_NAME,
            MANIFEST_COMPARISON_CUT_OFF_DATE_START,
            MANIFEST_COMPARISON_CUT_OFF_DATE_END,
            "2",
            "full",
            "historic",
            "/import_prefix",
            "/export_prefix",
            glue_client_mock())

        glue_client_mock.start_job_run.assert_called_once_with(
                JobName=ETL_GLUE_JOB_NAME,
                Arguments={
                              "--cut_off_time_start": MANIFEST_COMPARISON_CUT_OFF_DATE_START,
                              "--cut_off_time_end": MANIFEST_COMPARISON_CUT_OFF_DATE_END,
                              "--margin_of_error": str(2),
                              "--import_type": "full",
                              "--snapshot_type": "historic",
                              "--import_prefix": "import_prefix",
                              "--export_prefix": "export_prefix",
                              "--enable-metrics": "",
                          },
            )

if __name__ == "__main__":
    unittest.main()
