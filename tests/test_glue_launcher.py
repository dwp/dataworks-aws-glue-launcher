#!/usr/bin/env python3

"""glue_launcher_lambda"""
import argparse
import logging
import os
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import call, MagicMock

import boto3
import pytest
from glue_launcher_lambda import glue_launcher
from moto import mock_s3

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
args.job_queue_dependencies = ["testqueue", "queuetest"]

# Fetch table values
args.etl_glue_job_name = "jobName"
args.manifest_comparison_margin_of_error_minutes = "2"
args.manifest_comparison_snapshot_type = "incremental"
args.manifest_comparison_import_type = "streaming_main"
args.manifest_s3_input_location_import = "/import"
args.manifest_s3_input_location_export = "/export"
args.manifest_comparison_cut_off_date_start = "1983-11-15T09:09:55.000"
args.manifest_comparison_cut_off_date_end = "2099-11-15T09:09:55.000"
args.manifest_missing_imports_table_name = "missing_imports"
args.manifest_missing_exports_table_name = "missing_exports"
args.manifest_counts_parquet_table_name = "counts"
args.manifest_mismatched_timestamps_table_name = "mismatched_timestamps"
args.manifest_s3_input_parquet_location_missing_import = "/missing_imports"
args.manifest_s3_input_parquet_location_missing_export = "/missing_exports"
args.manifest_s3_input_parquet_location_counts = "/counts"
args.manifest_s3_input_parquet_location_mismatched_timestamps = "/mismatched_timestamps"
args.manifest_s3_output_location = "s3://bucket/output_location"
args.manifest_s3_bucket = "bucket"
args.manifest_s3_prefix = "output_location"


class TestRetriever(unittest.TestCase):
    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
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
    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_batch_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_batch_queue_jobs_present_no_further_action(
        self,
        mock_logger,
        get_parameters_mock,
        setup_logging_mock,
        get_batch_client_mock,
        get_and_validate_job_details_mock,
        running_batch_tasks_mock,
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
        with open(
            os.path.join(SQL_FILE_LOCATION, "create-parquet-table.sql"), "r"
        ) as f:
            base_create_parquet_query = f.read()

        with open(
            os.path.join(SQL_FILE_LOCATION, "create-missing-import-table.sql"),
            "r",
        ) as f:
            base_create_missing_import_query = f.read()

        with open(
            os.path.join(SQL_FILE_LOCATION, "create-missing-export-table.sql"),
            "r",
        ) as f:
            base_create_missing_export_query = f.read()

        with open(os.path.join(SQL_FILE_LOCATION, "create-count-table.sql"), "r") as f:
            base_create_count_query = f.read()

        expected = [
            [
                args.manifest_missing_imports_table_name,
                base_create_parquet_query,
                args.manifest_s3_input_parquet_location_missing_import,
            ],
            [
                args.manifest_missing_exports_table_name,
                base_create_missing_import_query,
                args.manifest_s3_input_parquet_location_missing_export,
            ],
            [
                args.manifest_counts_parquet_table_name,
                base_create_missing_export_query,
                args.manifest_s3_input_parquet_location_counts,
            ],
            [
                args.manifest_mismatched_timestamps_table_name,
                base_create_count_query,
                args.manifest_s3_input_parquet_location_mismatched_timestamps,
            ],
        ]
        actual = glue_launcher.fetch_table_creation_sql_files(SQL_FILE_LOCATION, args)
        assert (
            expected == actual
        ), f"Expected does not equal actual. Expected '{expected}' but got '{actual}'"

    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
    def test_batch_queue_jobs_empty_fetch_table_drop_sql(
        self,
        get_and_validate_job_details_mock,
    ):
        with open(os.path.join(SQL_FILE_LOCATION, "drop-table.sql"), "r") as f:
            base_drop_query = f.read()

        expected = base_drop_query
        actual = glue_launcher.fetch_table_drop_sql_file(SQL_FILE_LOCATION, args)

        assert (
            expected == actual
        ), f"Expected does not equal actual. Expected '{expected}' but got '{actual}'"

    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_batch_client")
    def test_batch_tasks_running(self, batch_client_mock, logger):
        batch_client_mock.list_jobs.side_effect = [
            {"jobSummaryList": [3]},
            {"jobSummaryList": [1], "nextToken": "1400"},
            {"jobSummaryList": [2]},
            {"jobSummaryList": [1, 2], "nextToken": "1500"},
            {"jobSummaryList": [2]},
            {"jobSummaryList": [1]},
            {"jobSummaryList": [1]},
        ]

        response = glue_launcher.check_running_batch_tasks(
            "job_queue", batch_client_mock
        )

        status_calls = [
            call(jobQueue="job_queue", jobStatus="SUBMITTED"),
            call(jobQueue="job_queue", jobStatus="PENDING"),
            call(jobQueue="job_queue", jobStatus="RUNNING"),
            call(jobQueue="job_queue", jobStatus="PENDING", nextToken="1400"),
            call(jobQueue="job_queue", jobStatus="RUNNABLE"),
            call(jobQueue="job_queue", jobStatus="RUNNABLE", nextToken="1500"),
            call(jobQueue="job_queue", jobStatus="STARTING"),
        ]
        batch_client_mock.list_jobs.assert_has_calls(status_calls, True)

        expected_call_count = 8

        assert (
            response == expected_call_count
        ), f"Response is not equal to {expected_call_count}"

    @mock.patch("glue_launcher_lambda.glue_launcher.execute_athena_query")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_recreate_single_table(
        self,
        mock_logger,
        execute_athena_mock,
    ):
        athena_client_mock = mock

        with open(os.path.join(SQL_FILE_LOCATION, "drop-table.sql"), "r") as f:
            base_drop_query = f.read()

        tables = [
            [
                args.manifest_missing_imports_table_name,
                """
                CREATE EXTERNAL TABLE IF NOT EXISTS [table_name]
                LOCATION '[s3_input_location]'
                """,
                args.manifest_s3_input_parquet_location_missing_import,
            ]
        ]

        drop_query = base_drop_query.replace(
            "[table_name]", args.manifest_missing_imports_table_name
        )
        create_query = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {args.manifest_missing_imports_table_name}
                LOCATION '{args.manifest_s3_input_parquet_location_missing_import}/'
                """

        actual = glue_launcher.recreate_sql_tables(
            tables, base_drop_query, athena_client_mock
        )

        execute_athena_mock_calls = [
            call(args.manifest_s3_output_location, drop_query, athena_client_mock),
            call(args.manifest_s3_output_location, create_query, athena_client_mock),
        ]

        execute_athena_mock.assert_has_calls(execute_athena_mock_calls)

    @mock.patch("glue_launcher_lambda.glue_launcher.execute_athena_query")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_recreate_multiple_tables(
        self,
        mock_logger,
        execute_athena_mock,
    ):
        athena_client_mock = mock

        with open(os.path.join(SQL_FILE_LOCATION, "drop-table.sql"), "r") as f:
            base_drop_query = f.read()

        tables = [
            [
                args.manifest_missing_imports_table_name,
                "CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] LOCATION '[s3_input_location]'",
                args.manifest_s3_input_parquet_location_missing_import,
            ],
            [
                args.manifest_missing_exports_table_name,
                "CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] LOCATION '[s3_input_location]'",
                args.manifest_s3_input_parquet_location_missing_export,
            ],
        ]

        glue_launcher.recreate_sql_tables(tables, base_drop_query, athena_client_mock)

        missing_imports_drop_query = (
            f"DROP * FROM {args.manifest_missing_imports_table_name};\n"
        )
        missing_imports_create_query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {args.manifest_missing_imports_table_name} LOCATION '{args.manifest_s3_input_parquet_location_missing_import}/'"

        missing_exports_drop_query = (
            f"DROP * FROM {args.manifest_missing_exports_table_name};\n"
        )
        missing_exports_create_query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {args.manifest_missing_exports_table_name} LOCATION '{args.manifest_s3_input_parquet_location_missing_export}/'"

        execute_athena_mock_calls = [
            call(
                args.manifest_s3_output_location,
                missing_imports_drop_query,
                athena_client_mock,
            ),
            call(
                args.manifest_s3_output_location,
                missing_imports_create_query,
                athena_client_mock,
            ),
            call(
                args.manifest_s3_output_location,
                missing_exports_drop_query,
                athena_client_mock,
            ),
            call(
                args.manifest_s3_output_location,
                missing_exports_create_query,
                athena_client_mock,
            ),
        ]

        execute_athena_mock.assert_has_calls(execute_athena_mock_calls)

    @mock.patch("glue_launcher_lambda.glue_launcher.get_glue_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_execute_manifest_glue_job(self, mock_logger, glue_client_mock):
        glue_client_mock.start_job_run = MagicMock()
        glue_client_mock.start_job_run.return_value = {"JobRunId": "12"}

        glue_launcher.execute_manifest_glue_job(
            ETL_GLUE_JOB_NAME,
            MANIFEST_COMPARISON_CUT_OFF_DATE_START,
            MANIFEST_COMPARISON_CUT_OFF_DATE_END,
            MANIFEST_COMPARISON_CUT_OFF_DATE_END,
            snapshot_type="full",
            import_type="historic",
            import_prefix="/import_prefix/",
            export_prefix="/export_prefix/",
            glue_client=glue_client_mock,
        )

        glue_client_mock.start_job_run.assert_called_once_with(
            JobName=ETL_GLUE_JOB_NAME,
            Arguments={
                "--cut_off_time_start": MANIFEST_COMPARISON_CUT_OFF_DATE_START,
                "--cut_off_time_end": MANIFEST_COMPARISON_CUT_OFF_DATE_END,
                "--margin_of_error": MANIFEST_COMPARISON_CUT_OFF_DATE_END,
                "--import_type": "historic",
                "--snapshot_type": "full",
                "--import_prefix": "/import_prefix",
                "--export_prefix": "/export_prefix",
                "--enable-metrics": "",
            },
        )

    @mock.patch("glue_launcher_lambda.glue_launcher.poll_athena_query_status")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_athena_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    def test_athena_executions(
        self,
        mock_logger,
        mock_setup_logger,
        get_params_mock,
        athena_client_mock,
        poll_athena_mock,
    ):
        get_params_mock.return_value = args
        athena_client_mock.start_query_execution.return_value = {
            "QueryExecutionId": "12"
        }

        poll_athena_mock.return_value = "SUCCEEDED"

        base_drop_query = "DROP * FROM test_table;"
        glue_launcher.execute_athena_query(
            args.manifest_s3_output_location, base_drop_query, athena_client_mock
        )

        athena_client_mock.start_query_execution.assert_called_with(
            QueryString="DROP * FROM test_table;",
            ResultConfiguration={"OutputLocation": "s3://bucket/output_location"},
        )
        athena_client_mock.get_query_results.assert_called_with(QueryExecutionId="12")
        poll_athena_mock.assert_called_with("12", athena_client_mock)

    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_athena_client")
    def test_athena_query_status_polling(self, athena_client_mock, mock_logger):
        athena_client_return = [
            {"QueryExecution": {"Status": {"State": "JUNK"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        ]
        athena_client_mock.get_query_execution.side_effect = athena_client_return

        result = glue_launcher.poll_athena_query_status("12", athena_client_mock)

        get_query_exec_calls = [
            call(QueryExecutionId="12"),
            call(QueryExecutionId="12"),
        ]
        athena_client_mock.get_query_execution.assert_has_calls(get_query_exec_calls)

        assert result == "SUCCEEDED", f"Result '{result}' is not SUCCEEDED."

    @mock.patch("glue_launcher_lambda.glue_launcher.get_today_midnight")
    def test_yesterday_midnight(self, midnight_mock):
        midnight_mock.return_value = datetime.strptime(
            "2021-07-27T00:00:00.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = datetime.strptime(
            "2021-07-26T00:00:00.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )
        actual = glue_launcher.get_previous_midnight()
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    @mock.patch("glue_launcher_lambda.glue_launcher.get_current_date")
    def test_midnight(self, current_date_mock):
        current_date_mock.return_value = datetime.strptime(
            "2021-07-27T05:38:54.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = datetime.strptime(
            "2021-07-27T00:00:00.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )
        actual = glue_launcher.get_today_midnight()
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    def test_generate_ms_epoch(self):
        datetime_obj = datetime.strptime(
            "2021-07-27T05:38:54.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = 1627364334000
        actual = glue_launcher.generate_ms_epoch_from_timestamp(datetime_obj)
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    def test_generate_ms_epoch_with_zero_minutes(self):
        datetime_obj = datetime.strptime(
            "2021-07-27T05:38:54.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = 1627364454000
        actual = glue_launcher.generate_ms_epoch_from_timestamp(datetime_obj, 2)
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    def test_generate_ms_epoch_additional_minutes(self):
        datetime_obj = datetime.strptime(
            "2021-07-27T05:38:54.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = 1627364454000
        actual = glue_launcher.generate_ms_epoch_from_timestamp(datetime_obj, 2)
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    def test_generate_ms_epoch(self):
        datetime_obj = datetime.strptime(
            "2021-07-27T05:38:54.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = 1627364334000
        actual = glue_launcher.generate_ms_epoch_from_timestamp(datetime_obj)
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    def test_generate_ms_epoch_additional_minutes(self):
        datetime_obj = datetime.strptime(
            "2021-07-27T05:38:54.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )

        expected = 1627364454000
        actual = glue_launcher.generate_ms_epoch_from_timestamp(datetime_obj, 2)
        assert (
            expected == actual
        ), f"Expected '{expected}' does not match actual '{actual}'"

    @mock.patch("glue_launcher_lambda.glue_launcher.execute_manifest_glue_job")
    @mock.patch("glue_launcher_lambda.glue_launcher.check_running_batch_tasks")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_batch_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_escaped_json_string")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    def test_handler_flow_jobs_in_queue(
        self,
        parameters_mock,
        setup_logging,
        logger,
        dumped_event,
        job_details_validator,
        batch_mock,
        running_batch_tasks,
        execute_glue,
    ):
        batch_mock.return_value = MagicMock()

        parameters_mock.return_value = args
        dumped_event.return_value = "{}"
        job_details_validator.return_value = {
            "jobName": "job",
            "status": "SUCCEEDED",
            "jobQueue": "testqueue",
        }

        running_batch_tasks.side_effect = [1, 3]

        event = {"event": "details"}

        calls = [
            call("testqueue", batch_mock.return_value),
            call("queuetest", batch_mock.return_value),
        ]

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            glue_launcher.handler(event, None)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 0
        running_batch_tasks.assert_has_calls(calls)
        execute_glue.assert_not_called()

    @mock.patch("glue_launcher_lambda.glue_launcher.execute_manifest_glue_job")
    @mock.patch("glue_launcher_lambda.glue_launcher.check_running_batch_tasks")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_batch_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_escaped_json_string")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    def test_handler_flow_jobs_in_queue_and_ignore_as_false(
        self,
        parameters_mock,
        setup_logging,
        logger,
        dumped_event,
        job_details_validator,
        batch_mock,
        running_batch_tasks,
        execute_glue,
    ):
        batch_mock.return_value = MagicMock()

        parameters_mock.return_value = args
        dumped_event.return_value = "{}"
        job_details_validator.return_value = {
            "jobName": "job",
            "status": "SUCCEEDED",
            "jobQueue": "testqueue",
            "ignoreBatchChecks": "false",
        }

        running_batch_tasks.side_effect = [1, 3]

        event = {"event": "details"}

        calls = [
            call("testqueue", batch_mock.return_value),
            call("queuetest", batch_mock.return_value),
        ]

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            glue_launcher.handler(event, None)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 0
        running_batch_tasks.assert_has_calls(calls)
        execute_glue.assert_not_called()

    @mock.patch("glue_launcher_lambda.glue_launcher.clear_manifest_output")
    @mock.patch("glue_launcher_lambda.glue_launcher.generate_ms_epoch_from_timestamp")
    @mock.patch("glue_launcher_lambda.glue_launcher.execute_manifest_glue_job")
    @mock.patch("glue_launcher_lambda.glue_launcher.recreate_sql_tables")
    @mock.patch("glue_launcher_lambda.glue_launcher.fetch_table_drop_sql_file")
    @mock.patch("glue_launcher_lambda.glue_launcher.fetch_table_creation_sql_files")
    @mock.patch("glue_launcher_lambda.glue_launcher.check_running_batch_tasks")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_glue_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_athena_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_batch_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_escaped_json_string")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    def test_handler_flow_no_jobs_in_queue(
        self,
        parameters_mock,
        setup_logging,
        logger,
        dumped_event,
        job_details_validator,
        batch_client_mock,
        athena_mock,
        glue_mock,
        running_batch_tasks,
        fetch_sql,
        drop_sql,
        recreate_tables,
        execute_glue,
        epoch_mock,
        clear_output_mock,
    ):
        batch_client_mock.return_value = MagicMock()
        athena_mock.return_value = MagicMock()
        glue_mock.return_value = MagicMock()

        parameters_mock.return_value = args
        dumped_event.return_value = "{}"
        job_details_validator.return_value = {
            "jobName": "job",
            "status": "SUCCEEDED",
            "jobQueue": "testqueue",
        }

        running_batch_tasks.side_effect = [0, 0]

        event = {"event": "details"}

        check_batch_calls = [
            call("testqueue", batch_client_mock.return_value),
            call("queuetest", batch_client_mock.return_value),
        ]

        fetch_sql.return_value = ["tables"]
        drop_sql.return_value = ["drop"]

        epoch_mock_calls = [
            call(args.manifest_comparison_cut_off_date_start),
            call(args.manifest_comparison_cut_off_date_end),
            call(args.manifest_comparison_cut_off_date_end, 2),
        ]
        epoch_mock.side_effect = ["12345", "23456", "23458"]

        glue_launcher.handler(event, None)

        clear_output_mock.assert_has_calls(
            [
                call("bucket", "output_location/queries"),
                call("bucket", "output_location/templates"),
                call("bucket", "output_location/results"),
            ]
        )

        running_batch_tasks.assert_has_calls(check_batch_calls)
        assert running_batch_tasks.call_count == len(check_batch_calls)
        epoch_mock.assert_has_calls(epoch_mock_calls)
        assert epoch_mock.call_count == len(epoch_mock_calls)

        fetch_sql.assert_called_with("sql", parameters_mock.return_value)
        drop_sql.assert_called_with("sql", parameters_mock.return_value)
        recreate_tables.assert_called_with(
            ["tables"], ["drop"], athena_mock.return_value
        )
        execute_glue.assert_called_with(
            "jobName",
            "12345",
            "23456",
            "23458",
            "incremental",
            "streaming_main",
            "/import",
            "/export",
            glue_mock.return_value,
        )

    @mock.patch("glue_launcher_lambda.glue_launcher.clear_manifest_output")
    @mock.patch("glue_launcher_lambda.glue_launcher.generate_ms_epoch_from_timestamp")
    @mock.patch("glue_launcher_lambda.glue_launcher.execute_manifest_glue_job")
    @mock.patch("glue_launcher_lambda.glue_launcher.recreate_sql_tables")
    @mock.patch("glue_launcher_lambda.glue_launcher.fetch_table_drop_sql_file")
    @mock.patch("glue_launcher_lambda.glue_launcher.fetch_table_creation_sql_files")
    @mock.patch("glue_launcher_lambda.glue_launcher.check_running_batch_tasks")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_glue_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_athena_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_batch_client")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_and_validate_job_details")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_escaped_json_string")
    @mock.patch("glue_launcher_lambda.glue_launcher.logger")
    @mock.patch("glue_launcher_lambda.glue_launcher.setup_logging")
    @mock.patch("glue_launcher_lambda.glue_launcher.get_parameters")
    def test_handler_with_batch_override(
        self,
        parameters_mock,
        setup_logging,
        logger,
        dumped_event,
        job_details_validator,
        batch_client_mock,
        athena_mock,
        glue_mock,
        running_batch_tasks,
        fetch_sql,
        drop_sql,
        recreate_tables,
        execute_glue,
        epoch_mock,
        clear_output_mock,
    ):
        athena_mock.return_value = MagicMock()
        glue_mock.return_value = MagicMock()

        parameters_mock.return_value = args
        dumped_event.return_value = "{}"
        job_details_validator.return_value = {
            "jobName": "job",
            "status": "SUCCEEDED",
            "jobQueue": "testqueue",
            "ignoreBatchChecks": "true",
        }

        event = {"event": "details"}

        fetch_sql.return_value = ["tables"]
        drop_sql.return_value = ["drop"]

        epoch_mock_calls = [
            call(args.manifest_comparison_cut_off_date_start),
            call(args.manifest_comparison_cut_off_date_end),
            call(args.manifest_comparison_cut_off_date_end, 2),
        ]
        epoch_mock.side_effect = ["12345", "23456", "23458"]

        glue_launcher.handler(event, None)

        clear_output_mock.assert_has_calls(
            [
                call("bucket", "output_location/queries"),
                call("bucket", "output_location/templates"),
                call("bucket", "output_location/results"),
            ]
        )

        running_batch_tasks.assert_not_called()

        epoch_mock.assert_has_calls(epoch_mock_calls)
        assert epoch_mock.call_count == len(epoch_mock_calls)

        fetch_sql.assert_called_with("sql", parameters_mock.return_value)
        drop_sql.assert_called_with("sql", parameters_mock.return_value)
        recreate_tables.assert_called_with(
            ["tables"], ["drop"], athena_mock.return_value
        )
        execute_glue.assert_called_with(
            "jobName",
            "12345",
            "23456",
            "23458",
            "incremental",
            "streaming_main",
            "/import",
            "/export",
            glue_mock.return_value,
        )

    @mock_s3
    @mock.patch("glue_launcher_lambda.glue_launcher.get_s3_client")
    def test_clear_manifest_output(self, mock_get_s3):
        glue_launcher.logger = logging.getLogger()
        bucket = "manifest_bucket"
        prefix = (
            "business-data/manifest/query-output_streaming_main_incremental/templates"
        )
        s3_client = boto3.client(service_name="s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )

        for n in range(5):
            s3_client.put_object(
                Body=f"File number {n}", Bucket=bucket, Key=f"{prefix}/{n}"
            )

        bucket_contents = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        num_files = bucket_contents["KeyCount"]

        self.assertEqual(num_files, 5)

        mock_get_s3.return_value = s3_client

        glue_launcher.clear_manifest_output(bucket, prefix)

        cleared_bucket_contents = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=prefix
        )
        num_cleared_files = cleared_bucket_contents["KeyCount"]

        self.assertEqual(num_cleared_files, 0)


if __name__ == "__main__":
    unittest.main()
