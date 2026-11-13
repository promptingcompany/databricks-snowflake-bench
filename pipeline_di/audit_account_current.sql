SELECT COUNT(*) FROM (SELECT account_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS c FROM {OUT}.dim_account GROUP BY account_id) x WHERE c <> 1
