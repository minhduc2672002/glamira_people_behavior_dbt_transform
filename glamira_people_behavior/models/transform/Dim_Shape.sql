WITH shape_cte AS (
  SELECT DISTINCT
    option.shapediamond AS shape_value
  FROM {{source('glamira','summary')}},
  UNNEST(option) AS  option
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['shape_value']) }} AS shape_key
    ,shape_value
FROM shape_cte