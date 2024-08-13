WITH collection_cte AS (
  SELECT DISTINCT
    option.Kollektion AS collection_value
  FROM {{source('glamira','summary')}},
  UNNEST(option) AS  option
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['collection_value']) }} AS collection_key
    ,collection_value
FROM collection_cte