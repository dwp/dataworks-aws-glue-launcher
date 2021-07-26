CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] (
    `id` string,
    `database` string,
    `collection` string,
    `export_timestamp` bigint,
    `export_component` string, 
    `export_type` string)
STORED AS PARQUET
LOCATION '[s3_input_location]'
tblproperties ("classification"="parquet","parquet.compress"="SNAPPY");