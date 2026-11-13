MERGE INTO {OUT}.fact_market f USING {OUT}.stg_market s ON f.dt = s.dt AND f.symbol = s.symbol
WHEN MATCHED THEN UPDATE SET close_price = s.close_price, high_price = s.high_price, low_price = s.low_price, volume = s.volume
WHEN NOT MATCHED THEN INSERT (dt, symbol, close_price, high_price, low_price, volume) VALUES (s.dt, s.symbol, s.close_price, s.high_price, s.low_price, s.volume)
