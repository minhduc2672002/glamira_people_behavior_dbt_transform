WITH alloy_cte AS (
  SELECT DISTINCT
    CASE 
      WHEN option.value_label IS NOT NULL  THEN  option.value_label
      ELSE option.alloy
    END AS alloy_value
  FROM {{source('glamira','summary')}},
  UNNEST(option) AS  option
  WHERE option.option_label = 'alloy' OR option.option_label IS NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['alloy_value']) }} AS alloy_key
    ,alloy_value as alloy_value_label
FROM alloy_cte