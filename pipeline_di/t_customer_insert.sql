INSERT INTO {OUT}.dim_customer (customer_sk, customer_id, version, tax_id, last_name, first_name, gender, dob, tier, city, state, postal, home_phone, mobile_phone, email, local_rate, national_rate, status, effective_ts, end_ts, is_current)
SELECT s.customer_id * 1000 + COALESCE(m.mv, 0) + 1, s.customer_id, COALESCE(m.mv, 0) + 1, s.tax_id, s.last_name, s.first_name, s.gender, s.dob, s.tier, s.city, s.state, s.postal, s.home_phone, s.mobile_phone, s.email, s.local_rate, s.national_rate,
       CASE WHEN s.action = 'DELETE' THEN 'INAC' ELSE 'ACTV' END, s.effective_ts, CAST('9999-12-31 00:00:00' AS TIMESTAMP), TRUE
FROM {OUT}.stg_customer s LEFT JOIN (SELECT customer_id, MAX(version) AS mv FROM {OUT}.dim_customer GROUP BY customer_id) m ON m.customer_id = s.customer_id
