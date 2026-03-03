SELECT
    dc.city,
    dd.year,
    dd.month,
    ROUND(SUM(fp.premium_amount), 2) AS premium_sum,
    COUNT(*) AS policy_count
FROM fact_policy_premium fp
JOIN dim_company dc
    ON fp.company_key = dc.company_key
JOIN dim_date dd
    ON fp.date_key = dd.date_key
GROUP BY 1, 2, 3
ORDER BY dd.year, dd.month, premium_sum DESC;
