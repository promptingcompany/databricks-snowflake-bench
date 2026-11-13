CREATE TABLE {OUT}.cdc AS
SELECT 'U' AS action, ss_ticket_number, ss_item_sk, ss_customer_sk, ss_store_sk, ss_sold_date_sk, d_date, d_year, d_moy,
       i_category, i_brand, c_customer_id, c_birth_year, ca_state, s_store_name, ss_quantity, ss_sales_price,
       CAST(ss_ext_sales_price * 1.10 AS DECIMAL(7,2)) AS ss_ext_sales_price, ss_ext_discount_amt, ss_net_paid
FROM {OUT}.t1_wide
WHERE ss_sold_date_sk BETWEEN {LO} AND {HI} AND MOD(ss_ticket_number * 31 + ss_item_sk, 20) = 0
UNION ALL
SELECT 'D' AS action, ss_ticket_number, ss_item_sk, ss_customer_sk, ss_store_sk, ss_sold_date_sk, d_date, d_year, d_moy, i_category, i_brand, c_customer_id, c_birth_year, ca_state, s_store_name, ss_quantity, ss_sales_price, ss_ext_sales_price, ss_ext_discount_amt, ss_net_paid
FROM {OUT}.t1_wide
WHERE ss_sold_date_sk BETWEEN {LO} AND {HI} AND MOD(ss_ticket_number * 17 + ss_item_sk, 200) = 1
  AND MOD(ss_ticket_number * 31 + ss_item_sk, 20) <> 0
UNION ALL
SELECT 'I' AS action, ss_ticket_number + {OFF} AS ss_ticket_number, ss_item_sk, ss_customer_sk, ss_store_sk, ss_sold_date_sk, d_date, d_year, d_moy,
       i_category, i_brand, c_customer_id, c_birth_year, ca_state, s_store_name, ss_quantity, ss_sales_price,
       ss_ext_sales_price, ss_ext_discount_amt, ss_net_paid
FROM {OUT}.t1_wide
WHERE ss_sold_date_sk BETWEEN {LO} AND {HI} AND MOD(ss_ticket_number * 13 + ss_item_sk, 50) = 2
