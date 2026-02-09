{{ config(materialized='table', schema='gold') }}

SELECT * FROM {{ ref('silver_car_sales') }}
