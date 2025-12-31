-- Version: 1.1
-- Author: Robert Slob

CREATE TABLE IF NOT EXISTS gharchive_project.core.gharchive_core (
    id NUMBER,
    type STRING,            
    public BOOLEAN,
    created_at TIMESTAMP_NTZ,
    
    -- Actor details
    actor_id NUMBER,
    actor_login STRING,
    
    -- Repo details
    repo_id NUMBER,
    repo_name STRING,
    
    updated_at TIMESTAMP_NTZ DEFAULT CONVERT_TIMEZONE('Europe/Amsterdam', CURRENT_TIMESTAMP())
)
CLUSTER BY (created_at);