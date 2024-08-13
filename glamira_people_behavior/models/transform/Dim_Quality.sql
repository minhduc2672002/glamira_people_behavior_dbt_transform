WITH quality_cte AS (
  SELECT DISTINCT
    option.quality AS quality,
    option.quality_label AS quality_label
  FROM {{source('glamira','summary')}},
  UNNEST(option) AS  option
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['quality','quality_label']) }} AS quality_key
    ,quality
    ,quality_label
FROM quality_cte