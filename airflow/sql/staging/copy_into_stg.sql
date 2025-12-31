-- Version: 1.1
-- Author: Robert Slob

COPY INTO gharchive_project.staging.gharchive_stg (
    id, type, public, created_at,
    actor, repo, payload, 
    ingested_at, s3_source_file
)
FROM (
    SELECT 
        $1:id::NUMBER, 
        $1:type::STRING, 
        $1:public::BOOLEAN, 
        TO_TIMESTAMP_NTZ(($1:created_at / 1000000)::INT), 
        $1:actor,           
        $1:repo,                            
        $1:payload, 
        CONVERT_TIMEZONE('Europe/Amsterdam', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
        METADATA$FILENAME
    FROM @gharchive_project.staging.s3_stage/gharchive/{{ ds }}/
)
FILE_FORMAT = (TYPE = 'PARQUET');