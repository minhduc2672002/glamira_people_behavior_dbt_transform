WITH checkout_success AS(
    SELECT
        time_stamp
        ,ip
        ,cart_products.*
    FROM {{source('glamira','summary')}},
    UNNEST(cart_products) AS cart_products
    WHERE collection = 'checkout_success'
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
        ,MAX(CASE 
            WHEN option.option_label='alloy' THEN option.value_label ELSE NULL
         END) AS alloy_value
        ,MAX(CASE 
            WHEN option.option_label='diamond' THEN option.value_label ELSE NULL
         END) AS diamond_value
    FROM checkout_success,
    UNNEST(option) AS option
    GROUP BY 1,2,3,4,5,6
)
,format_name AS (
    SELECT
        EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS date_time
        ,ip
        ,product_id
        ,diamond_value
        ,price
        ,amount 
        ,currency
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
    FROM transform
)
,genkey AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['date_time']) }}       AS date_key
        ,{{ dbt_utils.generate_surrogate_key(['ip']) }}             AS location_key
        ,product_id                                                 AS product_key
        ,{{ dbt_utils.generate_surrogate_key(['alloy_value']) }}    AS alloy_key
        ,{{ dbt_utils.generate_surrogate_key(['diamond_value']) }}  AS diamond_key
        ,price                                                      AS price
        ,amount                                                     AS amount
        ,currency                                                   AS currency
        ,CASE 
            WHEN currency != '' THEN exchange_rate_to_usd
            ELSE 1
        END AS exchange_rate_to_usd
    FROM format_name
    LEFT JOIN {{source('glamira','currency_exchange_rates')}} AS cer
     ON currency = cer.currency_code
)

SELECT
    gk.* 
    ,ROUND(price * amount * exchange_rate_to_usd ,2) AS total_price
FROM genkey AS gk


