CREATE TABLE {OUT}.t5_window AS
WITH m AS (
  SELECT c_customer_id, ca_state, d_year * 12 + d_moy AS mi, SUM(ss_ext_sales_price) AS rev
  FROM {OUT}.t1_wide
  WHERE c_customer_id IS NOT NULL AND d_year IS NOT NULL
  GROUP BY c_customer_id, ca_state, d_year * 12 + d_moy)
SELECT c_customer_id, ca_state, mi, rev,
       SUM(rev) OVER (PARTITION BY c_customer_id ORDER BY mi RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS trailing12,
       RANK() OVER (PARTITION BY ca_state, mi ORDER BY rev DESC) AS rnk
FROM m
