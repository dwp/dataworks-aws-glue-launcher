CREATE EXTERNAL TABLE IF NOT EXISTS [table_name] (
    `id` string,
    `database` string,
    `collection` string,
    `import_timestamp` bigint,
    `import_component` string, 
    `import_type` string)
STORED AS PARQUET
LOCATION '[s3_input_location]'
tblproperties ("classification"="parquet","parquet.compress"="SNAPPY");