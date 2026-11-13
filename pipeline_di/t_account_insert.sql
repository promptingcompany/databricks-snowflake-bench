INSERT INTO {OUT}.dim_account (account_sk, account_id, version, customer_id, customer_sk, account_name, tax_status, status, broker_id, effective_ts, end_ts, is_current)
SELECT s.account_id * 1000 + COALESCE(m.mv, 0) + 1, s.account_id, COALESCE(m.mv, 0) + 1, s.customer_id, c.customer_sk, s.account_name, s.tax_status,
       CASE WHEN s.action = 'DELETE' THEN 'INAC' ELSE 'ACTV' END, s.broker_id, s.effective_ts, CAST('9999-12-31 00:00:00' AS TIMESTAMP), TRUE
FROM {OUT}.stg_account s
LEFT JOIN (SELECT account_id, MAX(version) AS mv FROM {OUT}.dim_account GROUP BY account_id) m ON m.account_id = s.account_id
LEFT JOIN {OUT}.dim_customer c ON c.customer_id = s.customer_id AND c.is_current = TRUE
