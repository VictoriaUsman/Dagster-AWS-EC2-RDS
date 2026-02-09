{{ config(materialized='table', schema='bronze') }}

SELECT
    id,
    sale_date,
    car_model,
    region,
    CAST(
        REGEXP_REPLACE(
            REPLACE(REPLACE(REPLACE(sale_price, ' PHP', ''), '₱', ''), ',', ''),
            '[^0-9.]',
            '',
            'g'
        ) AS NUMERIC(12, 2)
    ) AS sale_price,
    INITCAP(TRIM(
        CASE
            WHEN customer_name LIKE '%,%' THEN
                TRIM(SPLIT_PART(customer_name, ',', 2)) || ' ' || TRIM(SPLIT_PART(customer_name, ',', 1))
            ELSE
                customer_name
        END
    )) AS customer_name
FROM {{ ref('stg_car_sales') }}
