
WITH product_cte AS(

    SELECT 
        product_id
    FROM {{source('glamira','summary')}}
    WHERE product_id IS NOT NULL

    UNION ALL

    SELECT 
        cart_prodcuts.product_id AS product_id
    FROM {{source('glamira','summary')}}, 
        UNNEST(cart_products) AS cart_prodcuts
    WHERE cart_prodcuts.product_id IS NOT NULL
)
,product_distinct AS (
    SELECT DISTINCT
        product_id 
    FROM product_cte 
)

SELECT 
    pd.product_id AS product_key
    ,CASE
        WHEN p.product_name IS NOT NULL THEN p.product_name
        ELSE 'unknown'
     END AS product_name
FROM product_distinct AS pd
LEFT JOIN {{source('glamira','products')}} AS p
 ON pd.product_id = p.product_id