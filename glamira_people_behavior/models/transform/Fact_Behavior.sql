WITH orginal_table AS(
    SELECT
        s.time_stamp
        ,s.ip
        ,cart_products.product_id
        ,cart_products.amount
        ,cart_products.currency
        ,cart_products.price
        ,option.option_label
        ,option.value_label
        ,NULL AS alloy
        ,NULL AS diamond
        ,NULL AS pearlcolor
        ,NULL AS stone
        ,s.collection
    FROM {{source('glamira','summary')}} AS s,
    UNNEST(cart_products) AS cart_products,
    UNNEST(cart_products.option) AS option

    UNION DISTINCT

    SELECT
        s.time_stamp
        ,s.ip
        ,s.product_id
        ,0 AS amount
        ,s.currency
        ,"0.00" AS price
        ,option.option_label
        ,option.value_label
        ,option.alloy
        ,option.diamond
        ,option.pearlcolor
        ,option.stone
        ,s.collection
    FROM {{source('glamira','summary')}} AS s,
    UNNEST(option) AS option
)
,transform AS (
   SELECT
        time_stamp
        ,ip
        ,product_id
        ,CASE 
             WHEN REGEXP_CONTAINS(price, r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') THEN CAST(REPLACE(price,',', '') AS FLOAT64)
             ELSE CAST(REGEXP_REPLACE(REPLACE(REPLACE(price,"'",''),'.','') , r"[٫,]", '.') AS FLOAT64) --invalid value is 1,88.00 and 1'88,00 60,00
        END AS price
        ,amount
        ,currency
        ,collection
        ,MAX(CASE 
                WHEN option_label='alloy' THEN value_label
                ELSE alloy
            END) AS alloy_value
        ,MAX(CASE 
                WHEN option_label='diamond' THEN value_label
                ELSE diamond 
            END) AS diamond_value
        ,MAX(CASE 
                WHEN option_label='pear' THEN value_label
                ELSE pearlcolor
         END) AS pearl_value
        ,MAX(CASE 
                WHEN option_label LIKE "stone%" THEN value_label
                ELSE stone
         END) AS stone_value
    FROM orginal_table AS ot
    GROUP BY 1,2,3,4,5,6,7
)
,format_name AS (
    SELECT
        time_stamp     
        ,ip             
        ,product_id  
        ,coalesce(CASE
        -- Nếu chuỗi chứa số, trích xuất phần trước số và số, sau đó thay thế ký tự `_` và `-` bằng khoảng trắng
              WHEN REGEXP_CONTAINS(alloy_value, r'[0-9]') THEN
                  CONCAT(
                      REGEXP_REPLACE(
                          SUBSTR(
                              alloy_value,
                              1,
                              GREATEST(1, REGEXP_INSTR(alloy_value, r'[0-9]') - 2)
                          ),
                          r'[_-]', ' '
                      ),
                      ' ',
                      REGEXP_EXTRACT(alloy_value, r'[0-9]+')
                  )
              WHEN alloy_value LIKE '' THEN NULL
              -- Nếu chuỗi không chứa số, thay thế ký tự `_` và `-` bằng khoảng trắng
              ELSE
                  REGEXP_REPLACE(alloy_value, r'[_-]', ' ')
          END,'unknown') AS alloy_value
        ,diamond_value 
        ,pearl_value    
        ,stone_value   
        ,price          
        ,amount        
        ,currency      
        ,collection 
    FROM transform
)
,format_name AS (
    SELECT
        time_stamp AS date_key
        ,{{ dbt_utils.generate_surrogate_key(['ip']) }}             AS location_key
        ,product_id                                                 AS product_key
        ,{{ dbt_utils.generate_surrogate_key(['alloy_value']) }}    AS alloy_key
        ,{{ dbt_utils.generate_surrogate_key(['diamond_value']) }}  AS diamond_key
        ,{{ dbt_utils.generate_surrogate_key(['pearl_value']) }}    AS pearl_key
        ,{{ dbt_utils.generate_surrogate_key(['stone_value']) }}    AS stone_key
        ,price                                                      AS price
        ,amount                                                     AS amount
        ,currency                                                   AS currency
        ,collection                                                 AS collection
    FROM transform
)
SELECT
    gk.* 
    ,ROUND(price * amount ,2) AS total_price
FROM genkey AS gk