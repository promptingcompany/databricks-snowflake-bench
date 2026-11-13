CREATE TABLE {OUT}.t3_cust360 AS
WITH s AS (SELECT ss_customer_sk AS cust, 'store' AS ch, SUM(ss_net_paid) AS paid, COUNT(*) AS n
           FROM store_sales WHERE ss_customer_sk IS NOT NULL GROUP BY ss_customer_sk),
     c AS (SELECT cs_bill_customer_sk AS cust, 'catalog' AS ch, SUM(cs_net_paid) AS paid, COUNT(*) AS n
           FROM catalog_sales WHERE cs_bill_customer_sk IS NOT NULL GROUP BY cs_bill_customer_sk),
     w AS (SELECT ws_bill_customer_sk AS cust, 'web' AS ch, SUM(ws_net_paid) AS paid, COUNT(*) AS n
           FROM web_sales WHERE ws_bill_customer_sk IS NOT NULL GROUP BY ws_bill_customer_sk),
     u AS (SELECT * FROM s UNION ALL SELECT * FROM c UNION ALL SELECT * FROM w)
SELECT u.cust, cu.c_customer_id, cu.c_birth_year, ca.ca_state,
       SUM(u.paid) AS total_paid, SUM(u.n) AS total_lines,
       SUM(CASE WHEN u.ch = 'store' THEN u.paid ELSE 0 END) AS store_paid,
       SUM(CASE WHEN u.ch = 'catalog' THEN u.paid ELSE 0 END) AS catalog_paid,
       SUM(CASE WHEN u.ch = 'web' THEN u.paid ELSE 0 END) AS web_paid
FROM u
JOIN customer cu ON u.cust = cu.c_customer_sk
LEFT JOIN customer_address ca ON cu.c_current_addr_sk = ca.ca_address_sk
GROUP BY u.cust, cu.c_customer_id, cu.c_birth_year, ca.ca_state
