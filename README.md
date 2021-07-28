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
| ENVIRONMENT | The environment the application is running in. 
| APPLICATION | The name of the application ie. glue_launcher_lambda |
| LOG_LEVEL   | INFO or Debug |
|JOB_QUEUE_DEPENDENCIES | Batch job queue names to check for running jobs |
|MISSING_IMPORTS_TABLE_NAME | Table name for missing imports ie. missing_imports |
|MISSING_EXPORTS_TABLE_NAME | Table name for missing exports ie. missing_exports |
|COUNTS_TABLE_NAME | Table name for manifest counts ie. counts |
|MISMATCHED_TIMESTAMPS_TABLE_NAME | Table name for manifest mismatches ie. mismatches |
|ETL_GLUE_JOB_NAME | Name of the target AWS Glue job to fire ie. etl_glue_job|
|MANIFEST_S3_INPUT_LOCATION_IMPORT_HISTORIC | |
|MANIFEST_S3_INPUT_LOCATION_EXPORT_HISTORIC | |
|MANIFEST_COMPARISON_CUT_OFF_DATE_START | |
|MANIFEST_COMPARISON_CUT_OFF_DATE_END | |
|MANIFEST_COMPARISON_MARGIN_OF_ERROR_MINUTES | |
|MANIFEST_COMPARISON_SNAPSHOT_TYPE | "full" or "incremental" |
|MANIFEST_COMPARISON_IMPORT_TYPE | "historic" or "|
|MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_IMPORT | |
|MANIFEST_S3_INPUT_PARQUET_LOCATION_MISSING_EXPORT | |
|MANIFEST_S3_INPUT_PARQUET_LOCATION_COUNTS | |
|MANIFEST_S3_INPUT_PARQUET_LOCATION_MISMATCHED_TIMESTAMPS | |
|MANIFEST_S3_OUTPUT_LOCATION | Output location on S3 for Athena query outputs |
