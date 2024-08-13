WITH pearl_cte AS (
  SELECT DISTINCT
    CASE 
      WHEN option.value_label IS NOT NULL  THEN  option.value_label
      ELSE option.pearlcolor
    END AS pearl_value
  FROM {{source('glamira','summary')}},
  UNNEST(option) AS  option
  WHERE option.option_label = 'pear' OR option.option_label IS NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['pearl_value']) }} AS pearl_key
    ,pearl_value as pearl_value_label
FROM pearl_cte