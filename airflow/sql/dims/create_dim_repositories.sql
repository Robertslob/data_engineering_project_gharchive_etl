-- Version: 1.1
-- Author: Robert Slob

-- View for unique repositories
CREATE OR REPLACE VIEW gharchive_project.core.dim_repositories AS
SELECT 
    repo_id, 
    MAX_BY(repo_name, created_at) AS repo_name,
    COUNT(*) AS total_events,
    MIN(created_at) AS first_seen,
    MAX(created_at) AS last_seen
FROM gharchive_project.core.gharchive_core
GROUP BY repo_id;