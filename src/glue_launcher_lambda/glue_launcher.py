import argparse
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timedelta

import boto3
import botocore

logger = None
args = None

FAILED_JOB_STATUS = "FAILED"
SUBMITTED_JOB_STATUS = "SUBMITTED"
PENDING_JOB_STATUS = "PENDING"
RUNNABLE_JOB_STATUS = "RUNNABLE"
STARTING_JOB_STATUS = "STARTING"
SUCCEEDED_JOB_STATUS = "SUCCEEDED"
RUNNING_JOB_STATUS = "RUNNING"

BATCH_CHECKS_OVERRIDE_KEY = "ignoreBatchChecks"

OPERATIONAL_JOB_STATUSES = [
    SUBMITTED_JOB_STATUS,
    PENDING_JOB_STATUS,
    RUNNABLE_JOB_STATUS,
    STARTING_JOB_STATUS,
    RUNNING_JOB_STATUS,
]
FINISHED_JOB_STATUSES = [SUCCEEDED_JOB_STATUS, FAILED_JOB_STATUS]

JOB_NAME_KEY = "jobName"
JOB_STATUS_KEY = "status"
JOB_QUEUE_KEY = "jobQueue"
JOB_STATUS_REASON_KEY = "statusReason"

JOB_CREATED_AT_KEY = ("createdAt", "Created at")
JOB_STARTED_AT_KEY = ("startedAt", "Started at")
JOB_STOPPED_AT_KEY = ("stoppedAt", "Stopped at")
OPTIONAL_TIME_KEYS = [JOB_CREATED_AT_KEY, JOB_STARTED_AT_KEY, JOB_STOPPED_AT_KEY]

SQL_LOCATION = "sql"
TIME_FORMAT_UNFORMATTED = "%Y-%m-%d %H:%M:%S"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

boto_client_config = botocore.config.Config(
    max_pool_connections=100, retries={"max_attempts": 10, "mode": "standard"}
)


def get_current_date():
    return datetime.utcnow()


def convert_time_to_script_time_format(time_string):
    """
    Convert time to script requirements from human readable
    time_string - Format of input YYYY-MM-DD HH:MM:SS allowed
    """
    return datetime.strptime(time_string, TIME_FORMAT_UNFORMATTED)


def get_today_midnight():
    return get_current_date().replace(hour=0, minute=0, second=0, microsecond=0)


def get_previous_midnight():
    return get_today_midnight() - timedelta(days=1)


def setup_logging(logger_level):
    """Set the default logger with json output."""
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)
    hostname = socket.gethostname()

    json_format = (
        f'{{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"environment": "{args.environment}", "application": "{args.application}", '
        f'"module": "%(module)s", "process":"%(process)s", '
        f'"thread": "[%(thread)s]", "host": "{hostname}" }}'
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level)
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def get_parameters():
    """Parse the supplied command line arguments.
    Returns:
        args: The parsed and validated command line arguments
    """
    parser = argparse.ArgumentParser()

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--environment", help="Environment value", default="UNSET_TEXT")
    parser.add_argument("--application", help="Application", default="UNSET_TEXT")
    parser.add_argument("--log-level", help="Log level for lambda", default="INFO")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    if "JOB_QUEUE_DEPENDENCIES_ARN_LIST" in os.environ:
        dependencies = os.environ["JOB_QUEUE_DEPENDENCIES_ARN_LIST"].split(",")
        _args.job_queue_dependencies = dependencies

    if "ETL_GLUE_JOB_NAME" in os.environ:
        _args.etl_glue_job_name = os.environ["ETL_GLUE_JOB_NAME"]

    if "MANIFEST_MISMATCHED_TIMESTAMPS_TABLE_NAME" in os.environ:
        _args.manifest_mismatched_timestamps_table_name = os.environ[
            "MANIFEST_MISMATCHED_TIMESTAMPS_TABLE_NAME"
        ]

    if "MANIFEST_MISSING_IMPORTS_TABLE_NAME" in os.environ:
        _args.manifest_missing_imports_table_name = os.environ[
            "MANIFEST_MISSING_IMPORTS_TABLE_NAME"
        ]

    if "MANIFEST_MISSING_EXPORTS_TABLE_NAME" in os.environ:
        _args.manifest_missing_exports_table_name = os.environ[
            "MANIFEST_MISSING_EXPORTS_TABLE_NAME"
        ]

    if "MANIFEST_COUNTS_PARQUET_TABLE_NAME" in os.environ:
        _args.manifest_counts_parquet_table_name = os.environ[
            "MANIFEST_COUNTS_PARQUET_TABLE_NAME"
        ]

    if "MANIFEST_S3_INPUT_LOCATION_IMPORT" in os.environ:
        _args.manifest_s3_input_location_import = os.environ[
            "MANIFEST_S3_INPUT_LOCATION_IMPORT"
        ]

    if "MANIFEST_S3_INPUT_LOCATION_EXPORT" in os.environ:
        _args.manifest_s3_input_location_export = os.environ[
            "MANIFEST_S3_INPUT_LOCATION_EXPORT"
        ]

    if "MANIFEST_COMPARISON_CUT_OFF_DATE_START" in os.environ:
        if (
            os.environ["MANIFEST_COMPARISON_CUT_OFF_DATE_START"].upper()
            == "PREVIOUS_DAY_MIDNIGHT"
        ):
            _args.manifest_comparison_cut_off_date_start = get_previous_midnight()
        else:
            _args.manifest_comparison_cut_off_date_start = (
                convert_time_to_script_time_format(
                    os.environ["MANIFEST_COMPARISON_CUT_OFF_DATE_START"]
                )
            )
    else:
        _args.manifest_comparison_cut_off_date_start = get_previous_midnight()

    if "MANIFEST_COMPARISON_CUT_OFF_DATE_END" in os.environ:
        if (
            os.environ["MANIFEST_COMPARISON_CUT_OFF_DATE_END"].upper()
            == "TODAY_MIDNIGHT"
        ):
            _args.manifest_comparison_cut_off_date_end = get_today_midnight()
        else:
            _args.manifest_comparison_cut_off_date_end = (
                convert_time_to_script_time_format(
                    os.environ["MANIFEST_COMPARISON_CUT_OFF_DATE_END"]
                )
            )
    else:
        _args.manifest_comparison_cut_off_date_end = get_today_midnight()

    if "MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES" in os.environ:
        _args.manifest_comparison_margin_of_error_minutes = os.environ[
            "MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES"
        ]
    else:
        _args.manifest_comparison_margin_of_error_minutes = "2"

    if "MANIFEST_COMPARISON_SNAPSHOT_TYPE" in os.environ:
        _args.manifest_comparison_snapshot_type = os.environ[
            "MANIFEST_COMPARISON_SNAPSHOT_TYPE"
        ]

    if "MANIFEST_COMPARISON_IMPORT_TYPE" in os.environ:
        _args.manifest_comparison_import_type = os.environ[
            "MANIFEST_COMPARISON_IMPORT_TYPE"
        ]

    if "MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_IMPORT" in os.environ:
        _args.manifest_s3_input_parquet_location_missing_import = os.environ[
            "MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_IMPORT"
        ]

    if "MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_EXPORT" in os.environ:
        _args.manifest_s3_input_parquet_location_missing_export = os.environ[
            "MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_EXPORT"
        ]

    if "MANIFEST_S3_INPUT_PARQUET_LOCATION_COUNTS" in os.environ:
        _args.manifest_s3_input_parquet_location_counts = os.environ[
            "MANIFEST_S3_INPUT_PARQUET_LOCATION_COUNTS"
        ]

    if "MANIFEST_S3_INPUT_PARQUET_LOCATION_MISMATCHED_TIMESTAMPS" in os.environ:
        _args.manifest_s3_input_parquet_location_mismatched_timestamps = os.environ[
            "MANIFEST_S3_INPUT_PARQUET_LOCATION_MISMATCHED_TIMESTAMPS"
        ]

    if "MANIFEST_S3_OUTPUT_LOCATION" in os.environ:
        _args.manifest_s3_output_location = os.environ["MANIFEST_S3_OUTPUT_LOCATION"]

    if "MANIFEST_S3_BUCKET" in os.environ:
        _args.manifest_s3_bucket = os.environ["MANIFEST_S3_BUCKET"]

    if "MANIFEST_S3_PREFIX" in os.environ:
        _args.manifest_s3_prefix = os.environ["MANIFEST_S3_PREFIX"]

    if "MANIFEST_DELETION_PREFIXES" in os.environ:
        _args.manifest_deletion_prefixes = (
            os.environ["MANIFEST_DELETION_PREFIXES"].remove(" ", "").split(",")
        )

    return _args


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:
        escaped_string = json.dumps(json_string)

    return escaped_string


def get_and_validate_job_details(message):
    """Get the job name from the event.
    Arguments:
        event (dict): The event
    """

    dumped_message = get_escaped_json_string(message)
    logger.info(f'Validating message", "message_details": {dumped_message}')

    if "detail" not in message:
        raise KeyError("Message contains no 'detail' key")

    detail_dict = message["detail"]
    required_keys = [JOB_NAME_KEY, JOB_STATUS_KEY, JOB_QUEUE_KEY]

    for required_key in required_keys:
        if required_key not in detail_dict:
            error_string = f"Details dict contains no '{required_key}' key"
            raise KeyError(error_string)

    logger.info(
        f'Message has been validated", "message_details": {dumped_message}, "job_queue": "{detail_dict[JOB_QUEUE_KEY]}", '
        + f'"job_name": "{detail_dict[JOB_NAME_KEY]}", "job_status": "{detail_dict[JOB_STATUS_KEY]}'
    )

    return detail_dict


def generate_ms_epoch_from_timestamp(datetime_object, minutes_to_add=0):
    """Returns the 1970 epoch as a number from the given timestamp.

    Keyword arguments:
    datetime_object -- datetime object
    minutes_to_add -- if any minutes are to be added to the time, set to greater than 0
    """

    if minutes_to_add > 0:
        datetime_object = datetime_object + timedelta(minutes=minutes_to_add)

    return int((datetime_object - datetime(1970, 1, 1)).total_seconds() * 1000.0)


def get_batch_client():
    return boto3.client("batch", config=boto_client_config)


def get_athena_client():
    return boto3.client("athena", config=boto_client_config)


def get_glue_client():
    return boto3.client("glue", config=boto_client_config)


def get_s3_client():
    return boto3.client("s3", config=boto_client_config)


def check_running_batch_tasks(job_queue, batch_client):
    """Check the AWS Batch job queue, return count of tasks in each status"""

    operational_tasks = 0

    for status in OPERATIONAL_JOB_STATUSES:
        response = batch_client.list_jobs(
            jobQueue=job_queue,
            jobStatus=status,
        )

        operational_tasks += len(response["jobSummaryList"])
        logger.info(
            f'Listing running jobs, "job_queue": "{job_queue}", "status_check": "{status}", "operational_tasks": "{operational_tasks}'
        )

        next_token = response.get("nextToken", None)
        while next_token is not None:
            response = batch_client.list_jobs(
                jobQueue=job_queue, jobStatus=status, nextToken=next_token
            )
            operational_tasks += len(response["jobSummaryList"])
            logger.info(
                f'Listing running jobs of tokenised page, "job_queue": "{job_queue}", "status_check": "{status}", "operational_tasks": "{operational_tasks}'
            )

            next_token = response.get("nextToken", None)

    return operational_tasks


def clear_manifest_output(bucket, prefix):
    logger.info(f"Clearing prefix {prefix}")
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    if does_s3_key_exist(pages):

        key_to_delete = dict(Objects=[], Quiet=True)
        for item in pages.search("Contents"):
            key_to_delete["Objects"].append(dict(Key=item["Key"]))

            # flush once aws limit reached
            if len(key_to_delete["Objects"]) == 1000:
                logger.info(f"Deleting 1000 objects")
                response = s3_client.delete_objects(Bucket=bucket, Delete=key_to_delete)
                logger.info(f"Response from s3 deletion {response}")
                key_to_delete = dict(Objects=[])

        # flush rest
        if len(key_to_delete["Objects"]):
            logger.info(
                f"Deleting {len(key_to_delete['Objects'])} objects from prefix {prefix}"
            )
            s3_client.delete_objects(Bucket=bucket, Delete=key_to_delete)


def does_s3_key_exist(paginator_pages):
    key_count = 0
    for page in paginator_pages:
        key_count += page["KeyCount"]
        if key_count > 0:
            return True
    return False


def fetch_table_creation_sql_files(file_path, args):
    with open(os.path.join(file_path, "create-parquet-table.sql"), "r") as f:
        base_create_parquet_query = f.read()

    with open(
        os.path.join(file_path, "create-missing-import-table.sql"),
        "r",
    ) as f:
        base_create_missing_import_query = f.read()

    with open(
        os.path.join(file_path, "create-missing-export-table.sql"),
        "r",
    ) as f:
        base_create_missing_export_query = f.read()

    with open(os.path.join(file_path, "create-count-table.sql"), "r") as f:
        base_create_count_query = f.read()

    tables = [
        [
            args.manifest_missing_imports_table_name,
            base_create_missing_import_query,
            args.manifest_s3_input_parquet_location_missing_import,
        ],
        [
            args.manifest_missing_exports_table_name,
            base_create_missing_export_query,
            args.manifest_s3_input_parquet_location_missing_export,
        ],
        [
            args.manifest_counts_parquet_table_name,
            base_create_count_query,
            args.manifest_s3_input_parquet_location_counts,
        ],
        [
            args.manifest_mismatched_timestamps_table_name,
            base_create_parquet_query,
            args.manifest_s3_input_parquet_location_mismatched_timestamps,
        ],
    ]

    return tables


def fetch_table_drop_sql_file(file_path, args):
    with open(os.path.join(file_path, "drop-table.sql"), "r") as f:
        base_drop_query = f.read()
        return base_drop_query


def poll_athena_query_status(id, athena_client):
    """Polls athena for the status of a query.

    Keyword arguments:
    id -- the id of the query in athena
    """
    time_taken = 1
    while True:
        query_execution_resp = athena_client.get_query_execution(QueryExecutionId=id)

        state = query_execution_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            logger.info(
                f"Athena query execution finished in {str(time_taken)} seconds with status of '{state}'"
            )
            return state

        time.sleep(1)
        time_taken += 1


def execute_athena_query(output_location, query, athena_client):
    """Executes the given individual query against athena and return the result.

    Keyword arguments:
    output_location -- the s3 location to output the results of the execution to
    query -- the query to execute
    """
    logger.info(f"Executing query and sending output results to '{output_location}'")

    query_start_resp = athena_client.start_query_execution(
        QueryString=query, ResultConfiguration={"OutputLocation": output_location}
    )
    execution_state = poll_athena_query_status(
        query_start_resp["QueryExecutionId"], athena_client
    )

    if execution_state != "SUCCEEDED":
        raise KeyError(
            f"Athena query execution failed with final execution status of '{execution_state}'"
        )

    return athena_client.get_query_results(
        QueryExecutionId=query_start_resp["QueryExecutionId"]
    )


def recreate_sql_tables(tables, drop_query, athena_client):
    for table_details in tables:
        logger.info(f"Dropping table named '{table_details[0]}' if exists")
        table_drop_query = drop_query
        table_drop_query = table_drop_query.replace("[table_name]", table_details[0])

        execute_athena_query(
            args.manifest_s3_output_location, table_drop_query, athena_client
        )

        logger.info(
            f"Generating table named '{table_details[0]}' from S3 location of '{table_details[2]}'"
        )

        s3_location = (
            table_details[2]
            if table_details[2].endswith("/")
            else f"{table_details[2]}/"
        )

        create_query = table_details[1].replace("[table_name]", table_details[0])
        create_query = create_query.replace("[s3_input_location]", s3_location)

        execute_athena_query(
            args.manifest_s3_output_location, create_query, athena_client
        )


def execute_manifest_glue_job(
    job_name,
    cut_off_time_start,
    cut_off_time_end,
    margin_of_error,
    snapshot_type,
    import_type,
    import_prefix,
    export_prefix,
    glue_client,
):
    """Executes the given job in aws glue.
    Keyword arguments:
    job_name -- the name of the job to execute
    cut_off_time_start -- the time to not report any timestamps before (use None for all)
    cut_off_time_end -- the time to not report any timestamps after
    margin_of_error -- the margin of error time for the job
    snapshot_type -- the type of snapshots the manifest were generated from (full or incremental)
    import_type -- the type of import manifests with no space, i.e. "streaming_main" or "streaming_equality"
    import_prefix -- the base s3 prefix for the import data
    export_prefix -- the base s3 prefix for the export data
    """
    logger.info(f"Executing glue job with name of '{job_name}'")

    import_prefix = import_prefix.rstrip("/")
    export_prefix = export_prefix.rstrip("/")

    cut_off_time_start_qualified = (
        "0" if cut_off_time_start is None else str(cut_off_time_start)
    )

    logger.info(
        f"Start time for job is '{cut_off_time_start_qualified}', import type is {import_type}, "
        + f"snapshot type is {snapshot_type}, end time is '{cut_off_time_end}', "
        + f"import prefix of '{import_prefix}', export prefix of '{export_prefix}' "
        + f"and margin of error is '{margin_of_error}'"
    )

    job_run_start_result = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            "--cut_off_time_start": cut_off_time_start_qualified,
            "--cut_off_time_end": str(cut_off_time_end),
            "--margin_of_error": str(margin_of_error),
            "--import_type": import_type,
            "--snapshot_type": snapshot_type,
            "--import_prefix": import_prefix,
            "--export_prefix": export_prefix,
            "--enable-metrics": "",
        },
    )
    job_run_id = job_run_start_result["JobRunId"]
    logger.info(f"Glue job with name of {job_name} started run with id of {job_run_id}")


def handler(event, context):
    """Handle the event from AWS.
    Args:
    event (Object): The event details from AWS
    context (Object): The context info from AWS
    """
    global args
    global logger

    args = get_parameters()
    logger = setup_logging(args.log_level)

    logger.debug(f"Working from '{os.getcwd()}'")

    dumped_event = get_escaped_json_string(event)
    logger.info(f'Event", "event": {dumped_event}, "mode": "handler')

    detail_dict = get_and_validate_job_details(event)

    job_name = detail_dict[JOB_NAME_KEY]
    job_status = detail_dict[JOB_STATUS_KEY]
    job_queue = detail_dict[JOB_QUEUE_KEY]

    override_batch_checks = (
        BATCH_CHECKS_OVERRIDE_KEY in detail_dict
        and detail_dict[BATCH_CHECKS_OVERRIDE_KEY] == "true"
    )

    if job_status not in FINISHED_JOB_STATUSES:
        logger.info(
            f'Exiting normally as job status warrants no further action", '
            + f'"job_name": "{job_name}", "job_queue": "{job_queue}", "job_status": "{job_status}'
        )
        sys.exit(0)

    logger.info(f"Job status is a finished job status '{job_status}'")

    if override_batch_checks:
        logger.info(
            f"Overriding batch checks as 'override_batch_checks' is set to '{override_batch_checks}'"
        )
    else:
        batch_client = get_batch_client()

        operational_tasks = 0
        for dependency in args.job_queue_dependencies:
            logger.info(f"Checking running tasks for '{dependency}'")

            queue_tasks = check_running_batch_tasks(dependency, batch_client)
            operational_tasks += queue_tasks

        if operational_tasks > 0:
            logger.info(
                f'Exiting as job queues are still busy, no further action", '
                + f'"job_name": "{job_name}", "job_queue": "{job_queue}", "job_status": "{job_status}'
            )
            sys.exit(0)

        logger.info(
            f"Operational tasks is '{operational_tasks}', continuing to create Athena tables"
        )

    for prefix_to_clear in args.manifest_deletion_prefixes:
        clear_manifest_output(
            args.manifest_s3_bucket, f"{args.manifest_s3_prefix}/{prefix_to_clear}"
        )

    tables = fetch_table_creation_sql_files(SQL_LOCATION, args)

    base_drop_query = fetch_table_drop_sql_file(SQL_LOCATION, args)

    recreate_sql_tables(tables, base_drop_query, get_athena_client())
    logger.info(
        f"Created Athena tables. Launching glue job '{args.etl_glue_job_name}' now"
    )

    execute_manifest_glue_job(
        args.etl_glue_job_name,
        generate_ms_epoch_from_timestamp(args.manifest_comparison_cut_off_date_start),
        generate_ms_epoch_from_timestamp(args.manifest_comparison_cut_off_date_end),
        generate_ms_epoch_from_timestamp(
            args.manifest_comparison_cut_off_date_end,
            int(args.manifest_comparison_margin_of_error_minutes),
        ),
        args.manifest_comparison_snapshot_type,
        args.manifest_comparison_import_type,
        args.manifest_s3_input_location_import,
        args.manifest_s3_input_location_export,
        get_glue_client(),
    )
    logger.info("Launched Glue Job - exiting")


if __name__ == "__main__":
    try:
        args = get_parameters()
        logger = setup_logging("INFO")

        logger.info(os.getcwd())
        json_content = json.loads(open("resources/event.json", "r").read())
        handler(json_content, None)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')
