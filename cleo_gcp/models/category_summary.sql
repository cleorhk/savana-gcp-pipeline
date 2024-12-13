SELECT 
    p.category,
    SUM(c.quantity * c.price) AS total_sales,
    SUM(c.quantity) AS total_items_sold
FROM 
    carts c
INNER JOIN 
    products p ON c.product_id = p.product_id
GROUP BY 
    p.category