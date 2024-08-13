WITH dim_is_undersupply_backordered as (
SELECT
  TRUE AS is_undersupply_backordered_boolean,
  'Undersupply Backorder' AS is_undersupply_backordered

UNION ALL
SELECT
  FALSE AS is_undersupply_backordered_boolean,
  'Not Undersupply Backorder' AS is_undersupply_backordered
)

SELECT
FARM_FINGERPRINT(CONCAT(dim_is_undersupply_backordered.is_undersupply_backordered,',',dim_package_type.package_type_key) ) AS sales_order_line_indicator_key,
  dim_is_undersupply_backordered.is_undersupply_backordered_boolean,
  dim_is_undersupply_backordered.is_undersupply_backordered,
  dim_package_type.package_type_key,
  dim_package_type.package_type_name

FROM dim_is_undersupply_backordered
CROSS JOIN dec-project-12.learn_dbt.dim_package_type dim_package_type