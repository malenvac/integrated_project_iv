WITH cte_orders AS (SELECT oo.order_id,
                           oo.customer_id,
                           oo.order_delivered_customer_date,
                           oop.payment_value
                    FROM olist_orders oo
                             JOIN olist_order_payments oop ON oo.order_id = oop.order_id
                    WHERE oo.order_delivered_customer_date IS NOT NULL
                      AND oo.order_status = 'delivered'
                    GROUP BY oo.order_id)
SELECT STRFTIME('%m', order_delivered_customer_date) AS month_no,
       CASE STRFTIME('%m', order_delivered_customer_date)
           WHEN '01' THEN 'Jan'
           WHEN '02' THEN 'Feb'
           WHEN '03' THEN 'Mar'
           WHEN '04' THEN 'Apr'
           WHEN '05' THEN 'May'
           WHEN '06' THEN 'Jun'
           WHEN '07' THEN 'Jul'
           WHEN '08' THEN 'Aug'
           WHEN '09' THEN 'Sep'
           WHEN '10' THEN 'Oct'
           WHEN '11' THEN 'Nov'
           WHEN '12' THEN 'Dec'
           END                                       AS month,
       ROUND(SUM(CASE WHEN STRFTIME('%Y', order_delivered_customer_date) = '2016' THEN payment_value ELSE 0.00 END),
             2)                                      AS Year2016,
       ROUND(SUM(CASE WHEN STRFTIME('%Y', order_delivered_customer_date) = '2017' THEN payment_value ELSE 0.00 END),
             2)                                      AS Year2017,
       ROUND(SUM(CASE WHEN STRFTIME('%Y', order_delivered_customer_date) = '2018' THEN payment_value ELSE 0.00 END),
             2)                                      AS Year2018
FROM cte_orders
GROUP BY month_no
ORDER BY month_no;