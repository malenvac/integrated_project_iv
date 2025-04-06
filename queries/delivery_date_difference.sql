SELECT 
    oc.customer_state AS State,
    CAST(AVG(julianday(strftime('%Y-%m-%d', order_estimated_delivery_date)) - 
             julianday(strftime('%Y-%m-%d', order_delivered_customer_date))) 
         AS INTEGER) AS Delivery_Difference
FROM olist_orders oo 
JOIN olist_customers oc ON oo.customer_id = oc.customer_id
WHERE oo.order_status = 'delivered'
AND oo.order_delivered_customer_date IS NOT NULL
GROUP BY oc.customer_state 
ORDER BY Delivery_Difference;
