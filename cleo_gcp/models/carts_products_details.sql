 
 
 {{ config(
    materialized='table'
) }}

 SELECT 
    c.cart_id,
    c.user_id,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value,
    p.name AS product_name,
    p.category,
    p.brand
FROM 
    carts c
INNER JOIN 
    products p ON c.product_id = p.product_id