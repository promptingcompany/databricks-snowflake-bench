CREATE TABLE {OUT}.t4_changes AS
SELECT ss_ticket_number, ss_item_sk, ss_ext_sales_price * 1.10 AS new_price
FROM {OUT}.t1_wide
WHERE MOD(ss_ticket_number * 31 + ss_item_sk, 100) = 0
