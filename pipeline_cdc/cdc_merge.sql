MERGE INTO {OUT}.t1_wide AS t
USING {OUT}.cdc AS u
ON t.ss_ticket_number = u.ss_ticket_number AND t.ss_item_sk = u.ss_item_sk
WHEN MATCHED AND u.action = 'D' THEN DELETE
WHEN MATCHED AND u.action = 'U' THEN UPDATE SET ss_ext_sales_price = u.ss_ext_sales_price
WHEN NOT MATCHED AND u.action = 'I' THEN INSERT (ss_ticket_number, ss_item_sk, ss_customer_sk, ss_store_sk, ss_sold_date_sk, d_date, d_year, d_moy, i_category, i_brand, c_customer_id, c_birth_year, ca_state, s_store_name, ss_quantity, ss_sales_price, ss_ext_sales_price, ss_ext_discount_amt, ss_net_paid) VALUES (u.ss_ticket_number, u.ss_item_sk, u.ss_customer_sk, u.ss_store_sk, u.ss_sold_date_sk, u.d_date, u.d_year, u.d_moy, u.i_category, u.i_brand, u.c_customer_id, u.c_birth_year, u.ca_state, u.s_store_name, u.ss_quantity, u.ss_sales_price, u.ss_ext_sales_price, u.ss_ext_discount_amt, u.ss_net_paid)
