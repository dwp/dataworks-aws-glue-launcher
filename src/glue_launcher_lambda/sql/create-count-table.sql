CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] (
    `database_collection` string,
    `database` string,
    `collection` string,
    `imported_count` bigint,
    `exported_count` bigint,
    `exported_count_from_data_load` bigint,
    `exported_count_from_streaming_feed` bigint,
    `missing_imported_count` bigint,
    `missing_exported_count` bigint,
    `mismatched_timestamps_earlier_in_import_count` bigint,
    `mismatched_timestamps_earlier_in_export_count` bigint,
    `latest_import_timestamp` bigint,
    `earliest_import_timestamp` bigint,
    `latest_export_timestamp` bigint,
    `earliest_export_timestamp` bigint
)
STORED AS PARQUET
LOCATION '[s3_input_location]'
tblproperties ("classification"="parquet","parquet.compress"="SNAPPY");