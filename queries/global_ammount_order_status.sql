SELECT
    oo.order_status,
    COUNT(oo.order_status) AS Ammount
FROM olist_orders oo
GROUP BY
    oo.order_status;
