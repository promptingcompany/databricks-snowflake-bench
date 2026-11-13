INSERT INTO {OUT}.t2_daily
SELECT d_date, s_store_name, i_category, SUM(ss_ext_sales_price) AS revenue, SUM(ss_quantity) AS qty,
       SUM(ss_ext_discount_amt) AS discount, COUNT(*) AS lines
FROM {OUT}.t1_wide
WHERE d_date IN (SELECT DISTINCT d_date FROM {OUT}.cdc)
GROUP BY d_date, s_store_name, i_category
