-- Version: 1.1
-- Author: Robert Slob

CREATE OR REPLACE VIEW gharchive_project.marts.view_repo_popularity AS
SELECT 
    repo_id,
    created_at::DATE as event_date,
    SUM(CASE WHEN type = 'PushEvent' THEN 1 ELSE 0 END) as push_count,
    SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) as star_count
FROM gharchive_project.core.gharchive_core
WHERE type IN ('PushEvent', 'WatchEvent')
AND actor_login NOT LIKE '%[bot]%'
GROUP BY 1, 2;