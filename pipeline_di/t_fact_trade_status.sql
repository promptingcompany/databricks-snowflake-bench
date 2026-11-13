MERGE INTO {OUT}.fact_trade f USING (SELECT trade_id, status FROM (SELECT trade_id, status, ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY ts DESC, CASE WHEN status = 'SBMT' THEN 0 ELSE 1 END DESC) AS rn FROM {OUT}.stg_trade_history) x WHERE rn = 1) h
ON f.trade_id = h.trade_id WHEN MATCHED AND f.status <> h.status THEN UPDATE SET status = h.status
