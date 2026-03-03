WITH max_review_date AS (
    SELECT MAX(dd.date) AS max_date
    FROM fact_review fr
    JOIN dim_date dd
        ON fr.date_key = dd.date_key
),
recent_reviews AS (
    SELECT
        fr.company_key,
        fr.rating
    FROM fact_review fr
    JOIN dim_date dd
        ON fr.date_key = dd.date_key
    CROSS JOIN max_review_date m
    WHERE dd.date >= m.max_date - INTERVAL 90 DAY
)
SELECT
    dc.company_id,
    dc.name AS company_name,
    ROUND(AVG(rr.rating), 2) AS avg_rating_90d,
    COUNT(*) AS review_count_90d
FROM recent_reviews rr
JOIN dim_company dc
    ON rr.company_key = dc.company_key
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY avg_rating_90d DESC, review_count_90d DESC
LIMIT 20;
