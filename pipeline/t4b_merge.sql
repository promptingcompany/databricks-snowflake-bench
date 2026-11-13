MERGE INTO {OUT}.t1_wide AS t
USING {OUT}.t4_changes AS u
ON t.ss_ticket_number = u.ss_ticket_number AND t.ss_item_sk = u.ss_item_sk
WHEN MATCHED THEN UPDATE SET ss_ext_sales_price = u.new_price
