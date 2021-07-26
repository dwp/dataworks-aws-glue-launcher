CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] (
    `id` string,
    `database` string,
    `collection` string,
    `import_timestamp` bigint,
    `import_source` string, 
    `import_component` string, 
    `import_type` string,
    `export_timestamp` bigint,
    `export_source` string, 
    `export_component` string, 
    `export_type` string,
    `earliest_timestamp` bigint, 
    `latest_timestamp` bigint, 
    `earliest_manifest` string)
STORED AS PARQUET
LOCATION '[s3_input_location]'
tblproperties ("classification"="parquet","parquet.compress"="SNAPPY");