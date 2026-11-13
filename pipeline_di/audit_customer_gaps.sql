SELECT COUNT(*) FROM (SELECT end_ts, LEAD(effective_ts) OVER (PARTITION BY customer_id ORDER BY version) AS nxt FROM {OUT}.dim_customer) x WHERE nxt IS NOT NULL AND nxt <> end_ts
