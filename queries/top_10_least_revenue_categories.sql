SELECT 
    pcnt.product_category_name_english AS Category,
    COUNT(DISTINCT oo.order_id) AS Num_order,
    SUM(oop.payment_value) AS Revenue
FROM olist_orders oo 
JOIN olist_order_items ooi ON oo.order_id = ooi.order_id
JOIN olist_order_payments oop ON oo.order_id = oop.order_id
JOIN olist_products op ON ooi.product_id = op.product_id
JOIN product_category_name_translation pcnt ON op.product_category_name = pcnt.product_category_name
WHERE oo.order_status = 'delivered'
AND oo.order_delivered_customer_date IS NOT NULL
GROUP BY pcnt.product_category_name_english
ORDER BY Revenue ASC
LIMIT 10;
