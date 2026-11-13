INSERT INTO {OUT}.fact_trade (trade_id, account_sk, customer_sk, account_id, symbol, trade_type, status, quantity, bid_price, trade_price, fee, commission, tax, trade_ts, trade_date, batch)
SELECT t.trade_id, a.account_sk, c.customer_sk, t.account_id, t.symbol, t.trade_type, 'SBMT', t.quantity, t.bid_price, t.trade_price, t.fee, t.commission, t.tax, t.trade_ts, CAST(t.trade_ts AS DATE), {B}
FROM {OUT}.stg_trade t
LEFT JOIN {OUT}.dim_account a ON a.account_id = t.account_id AND t.trade_ts >= a.effective_ts AND t.trade_ts < a.end_ts
LEFT JOIN {OUT}.dim_customer c ON c.customer_id = a.customer_id AND t.trade_ts >= c.effective_ts AND t.trade_ts < c.end_ts
