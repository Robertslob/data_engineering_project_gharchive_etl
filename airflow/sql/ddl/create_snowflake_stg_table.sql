-- Version: 1.1
-- Author: Robert Slob

CREATE TABLE IF NOT EXISTS gharchive_project.staging.gharchive_stg (
    id NUMBER,
    type STRING,            
    public BOOLEAN,
    created_at TIMESTAMP_NTZ,
    actor VARIANT,
    repo VARIANT,
    payload VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Amsterdam', CURRENT_TIMESTAMP()),
    s3_source_file STRING
);