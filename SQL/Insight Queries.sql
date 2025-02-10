SELECT 
    dr.reason AS request_reason, 
    COUNT(f.request_id) AS total_requests
FROM fact_311_request f
JOIN dim_request_dtl dr ON f.request_dtl_key = dr.request_dtl_key
WHERE open_date >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)
GROUP BY dr.reason
ORDER BY total_requests DESC
LIMIT 5;


SELECT 
    dr.reason AS request_reason, 
    ROUND(AVG(TIMESTAMPDIFF(HOUR, open_date, closed_date)), 2) AS avg_resolution_time_hours
FROM fact_311_request f
JOIN dim_request_dtl dr ON f.request_dtl_key = dr.request_dtl_key
WHERE f.closed_date IS NOT NULL
GROUP BY dr.reason
ORDER BY avg_resolution_time_hours ASC;



SELECT 
    dr.department, 
    COUNT(*) AS total_requests, 
    SUM(CASE WHEN on_time = 'ONTIME' THEN 1 ELSE 0 END) AS on_time_requests,
    ROUND(SUM(CASE WHEN on_time = 'ONTIME' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS sla_compliance_rate
FROM fact_311_request f
JOIN dim_request_dtl dr ON f.request_dtl_key = dr.request_dtl_key
GROUP BY dr.department
ORDER BY sla_compliance_rate DESC;

SELECT neighborhood, request_reason, total_requests
FROM (
    SELECT 
        dl.neighborhood, 
        dr.reason AS request_reason, 
        COUNT(f.request_id) AS total_requests,
        RANK() OVER (PARTITION BY dl.neighborhood ORDER BY COUNT(f.request_id) DESC) AS rank_order
    FROM fact_311_request f
    JOIN dim_location dl ON f.location_key = dl.location_key
    JOIN dim_request_dtl dr ON f.request_dtl_key = dr.request_dtl_key
    GROUP BY dl.neighborhood, dr.reason
) AS RankedRequests
WHERE rank_order <= 3
ORDER BY neighborhood, rank_order;


