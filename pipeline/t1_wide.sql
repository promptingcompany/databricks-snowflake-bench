CREATE TABLE {OUT}.t1_wide AS
SELECT ss.ss_ticket_number, ss.ss_item_sk, ss.ss_customer_sk, ss.ss_store_sk, ss.ss_sold_date_sk,
       d.d_date, d.d_year, d.d_moy, i.i_category, i.i_brand, c.c_customer_id, c.c_birth_year,
       ca.ca_state, s.s_store_name, ss.ss_quantity, ss.ss_sales_price, ss.ss_ext_sales_price,
       ss.ss_ext_discount_amt, ss.ss_net_paid
FROM store_sales ss
LEFT JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
LEFT JOIN item i ON ss.ss_item_sk = i.i_item_sk
LEFT JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
LEFT JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
LEFT JOIN store s ON ss.ss_store_sk = s.s_store_sk
