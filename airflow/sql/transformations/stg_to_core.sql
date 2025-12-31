-- Version: 1.1
-- Author: Robert Slob

MERGE INTO gharchive_project.core.gharchive_core AS target
USING (
    SELECT 
        id, type, public, created_at,
        actor:id::NUMBER AS actor_id,
        actor:login::STRING AS actor_login,
        repo:id::NUMBER AS repo_id,
        repo:name::STRING AS repo_name,
        ingested_at
    FROM gharchive_project.staging.gharchive_stg
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at DESC) = 1
) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
    INSERT (id, type, public, created_at, actor_id, actor_login, repo_id, repo_name, updated_at)
    VALUES (source.id, source.type, source.public, source.created_at, source.actor_id, 
            source.actor_login, source.repo_id, source.repo_name, CONVERT_TIMEZONE('Europe/Amsterdam', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ);

-- Clear staging after successful merge
TRUNCATE TABLE gharchive_project.staging.gharchive_stg;