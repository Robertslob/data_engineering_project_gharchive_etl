-- Version: 1.1
-- Author: Robert Slob

USE DATABASE gharchive_project;

-- Get daily push and star counts for most popular repos of last week
CREATE OR REPLACE TABLE gharchive_project.sandbox.most_popular_last_week AS (

-- Determine the most starred repos of last week
WITH most_popular_last_week_temp AS (
    select 
        repo_ID, 
        sum(star_count) AS star_count 
    FROM marts.view_repo_popularity
    WHERE 1=1
        AND EVENT_DATE >= DATE_FROM_PARTS(2025, 12, 22) 
        AND EVENT_DATE <  DATE_FROM_PARTS(2025, 12, 29)
    GROUP BY 
        REPO_ID
    ORDER BY 
        star_count desc
    LIMIT 10
)

-- Get daily push and star counts for most popular repos of last week
SELECT 
    pop_view.REPO_ID, 
    dim_repo.REPO_NAME, 
    pop_view.event_date, 
    pop_view.push_count, 
    pop_view.star_count
FROM marts.view_repo_popularity AS pop_view
INNER JOIN most_popular_last_week_temp AS pop_last_week
    ON pop_view.repo_id = pop_last_week.repo_id
INNER JOIN core.dim_repositories AS dim_repo
    ON pop_last_week.repo_id = dim_repo.repo_id
order by 
    pop_view.repo_id, 
    pop_view.event_date asc
)