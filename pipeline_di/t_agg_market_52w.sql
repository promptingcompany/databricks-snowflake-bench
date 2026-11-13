CREATE OR REPLACE TABLE {OUT}.agg_market_52w AS
SELECT m.dt, m.symbol, m.close_price, c.day_idx,
       MAX(m.high_price) OVER (PARTITION BY m.symbol ORDER BY c.day_idx RANGE BETWEEN 364 PRECEDING AND CURRENT ROW) AS high_52w,
       MIN(m.low_price) OVER (PARTITION BY m.symbol ORDER BY c.day_idx RANGE BETWEEN 364 PRECEDING AND CURRENT ROW) AS low_52w
FROM {OUT}.fact_market m JOIN {OUT}.ref_calendar c ON c.cal_date = m.dt
