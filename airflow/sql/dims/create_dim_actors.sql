-- Version: 1.1
-- Author: Robert Slob

-- View for unique actors
CREATE OR REPLACE VIEW gharchive_project.core.dim_actors AS
SELECT 
    actor_id, 
    MAX_BY(actor_login, created_at) AS actor_login,
    COUNT(*) AS total_actions,
    MIN(created_at) AS first_active,
    MAX(created_at) AS last_active
FROM gharchive_project.core.gharchive_core
GROUP BY actor_id;