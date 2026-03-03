SELECT
    dc.industry,
    ROUND(AVG(fr.rating), 2) AS avg_rating,
    COUNT(*) AS review_count
FROM fact_review fr
JOIN dim_company dc
    ON fr.company_key = dc.company_key
GROUP BY 1
ORDER BY avg_rating DESC, review_count DESC;
