{{ config(materialized='table') }}

SELECT *
FROM {{ source('car_sales_raw', 'car_sales') }}
