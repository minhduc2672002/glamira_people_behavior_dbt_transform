WITH diamond_cte AS (
  SELECT DISTINCT
    CASE 
      WHEN option.value_label IS NOT NULL  THEN  option.value_label
      ELSE option.diamond
    END AS diamond_value
  FROM {{source('glamira','summary')}},
  UNNEST(option) AS  option
  WHERE option.option_label = 'diamond' OR option.option_label IS NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['diamond_value']) }} AS diamond_key
    ,diamond_value as diamond_value_label
FROM diamond_cte