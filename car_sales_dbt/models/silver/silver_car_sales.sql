{{ config(materialized='table', schema='silver') }}

WITH customer_totals AS (
    SELECT
        customer_name,
        SUM(sale_price) AS total_sales
    FROM {{ ref('bronze_car_sales') }}
    GROUP BY customer_name
),

tagged AS (
    SELECT
        b.*,
        ct.total_sales,
        CASE
            WHEN ct.total_sales >= 600000 THEN 'Valued Customer'
            WHEN ct.total_sales > 400000 THEN 'Regular'
            ELSE 'Not Frequent Buyer'
        END AS customer_tag
    FROM {{ ref('bronze_car_sales') }} b
    LEFT JOIN customer_totals ct ON b.customer_name = ct.customer_name
)

SELECT * FROM tagged
