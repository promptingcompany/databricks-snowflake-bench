SELECT COUNT(*) FROM (SELECT customer_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS c FROM {OUT}.dim_customer GROUP BY customer_id) x WHERE c <> 1
