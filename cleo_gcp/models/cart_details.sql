SELECT 
    c.cart_id,
    c.user_id,
    c.product_id,
    c.quantity,
    c.price,
    c.total_cart_value
FROM 
    carts c