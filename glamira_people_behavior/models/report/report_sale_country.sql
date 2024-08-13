SELECT
    l.country_name
    ,SUM(total_price) AS total_revenue
FROM {{ref('Fact_Sale')}} AS s
LEFT JOIN {{ref('Dim_Location')}} AS l
    ON l.location_key=s.location_key
GROUP BY l.country_name