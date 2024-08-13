SELECT
    DISTINCT
    {{ dbt_utils.generate_surrogate_key(['ip']) }} AS location_key
    ,ip
    ,CASE WHEN country_code = '-' THEN 'unknown' ELSE country_code END AS country_code
    ,CASE WHEN country_name = '-' THEN 'unknown' ELSE country_name END AS country_name
    ,CASE WHEN region_name = '-' THEN 'unknown' ELSE region_name END AS region_name
    ,CASE WHEN city_name = '-' THEN 'unknown' ELSE city_name END AS city_name
    ,CASE WHEN postal_code = '-' THEN 'unknown' ELSE postal_code END AS postal_code
FROM {{source('glamira','ip_to_location')}} 