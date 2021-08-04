# dataworks-aws-glue-launcher

## A Lambda which prepares and starts AWS Glue jobs once dependencies are met.

The lambda is designed to be fired upon CloudWatch event rules triggering on AWS Batch job status changes.
The lambda only cares about success or failure status codes, status codes such as pending, starting and running are ignored. The lambda will exit if the incoming event is of any of these statuses.
Modify `IGNORED_JOB_STATUSES` in the code to adjust which statuses are ignored.

If the event received has an AWS Batch job status of Succeeded or Failed, the lambda will check the dependency AWS Batch job queues to ensure they have no pending tasks.
If there are no pending tasks on the queues, the lambda will first drop the old manifest tables on S3 and recreate the Athena tables.
Then the lambda will start the AWS Glue job with its required parameters.

## SQL Files in use
| File | Explanation|
| --- | :--- |
| drop-table.sql | This runs twice and drops any existing tables for both import data and export data in order to start with a fresh table each time, this allows for schema changes
| create-table.sql | This creates two empty externals tables in Athena to represent the manifest data from the import set and the manifest data from the export set. The columns match the data in the files as described above.
| create-parquet-table.sql | This creates a big table with extra columns in AWS Glue which will be populated later on. This table is in a columnar format called Parquet which is easier for large scale data analysis to be performed on.

# Environment variables
|Property | Value|
|:---|---:|
|ENVIRONMENT | The environment the application is running in. 
|APPLICATION | The name of the application ie. glue_launcher_lambda |
|LOG_LEVEL   | INFO or Debug |
|JOB_QUEUE_DEPENDENCIES_ARN_LIST | Batch job queue ARNs to check for running jobs |
|ETL_GLUE_JOB_NAME | Name of the target AWS Glue job to fire ie. etl_glue_job|
|MANIFEST_MISMATCHED_TIMESTAMPS_TABLE_NAME | Table name for manifest mismatches ie. mismatches |
|MANIFEST_MISSING_IMPORTS_TABLE_NAME | Table name for missing imports ie. missing_imports |
|MANIFEST_MISSING_EXPORTS_TABLE_NAME | Table name for missing exports ie. missing_exports |
|MANIFEST_COUNTS_PARQUET_TABLE_NAME | Table name for manifest counts ie. counts |
|MANIFEST_S3_INPUT_LOCATION_IMPORT | S3 prefix for import location |
|MANIFEST_S3_INPUT_LOCATION_EXPORT | S3 prefix for export location |
|MANIFEST_COMPARISON_CUT_OFF_DATE_START | Lambda defaults to using previous day midnight. Override with a 'YYYY-MM-DD HH:MM:SS' |
|MANIFEST_COMPARISON_CUT_OFF_DATE_END | Lambda defaults to using today midnight. Override with a 'YYYY-MM-DD HH:MM:SS' |
|MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES | Margin of error for manifest comparison given in minutes. Default is 2 minutes. |
|MANIFEST_COMPARISON_SNAPSHOT_TYPE | "full" or "incremental" |
|MANIFEST_COMPARISON_IMPORT_TYPE | "historic" or "streaming_main" or "streaming_equality" |
|MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_IMPORT | Full S3 URI to missing import output location|
|MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_EXPORT | Full S3 URI to missing export output location |
|MANIFEST_S3_INPUT_PARQUET_LOCATION_COUNTS | Full S3 URI to counts output location |
|MANIFEST_S3_INPUT_PARQUET_LOCATION_MISMATCHED_TIMESTAMPS | Full S3 URI to mismatched timestamps output location |
|MANIFEST_S3_OUTPUT_LOCATION | Output location on S3 for Athena query outputs |
