-- models/users_with_carts_model.sql

{{ config(
    materialized='table'
) }}

SELECT 
    u.user_id,
    u.age,
    u.first_name,
    u.last_name,
    u.gender,
    u.postal_code,
    u.city,
    c.cart_id,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value
FROM 
    users u
INNER JOIN 
    carts c ON u.user_id = c.user_id