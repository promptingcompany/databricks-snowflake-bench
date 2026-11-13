CREATE OR REPLACE TABLE {OUT}.agg_position AS
SELECT account_id, symbol,
       SUM(CASE WHEN trade_type IN ('TMB', 'TLB') THEN quantity ELSE -quantity END) AS net_qty,
       SUM(quantity * trade_price) AS traded_value, COUNT(*) AS trades
FROM {OUT}.fact_trade WHERE status = 'CMPT' GROUP BY account_id, symbol
