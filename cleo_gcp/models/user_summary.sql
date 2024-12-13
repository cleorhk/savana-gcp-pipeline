SELECT 
    u.user_id,
    u.first_name,
    SUM(c.total_cart_value) AS total_spent,
    SUM(c.quantity) AS total_items,
    u.age,
    u.city
FROM 
    users u
LEFT JOIN 
    carts c ON u.user_id = c.user_id
GROUP BY 
    u.user_id, u.first_name, u.age, u.city