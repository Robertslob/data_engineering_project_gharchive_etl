-- Version: 1.1
-- Author: Robert Slob

CREATE OR REPLACE VIEW gharchive_project.marts.view_daily_trends AS
SELECT 
    created_at::DATE AS activity_date,
    type AS event_type,
    COUNT(*) AS event_count
FROM gharchive_project.core.gharchive_core
WHERE actor_login NOT LIKE '%[bot]%'
GROUP BY 1, 2;