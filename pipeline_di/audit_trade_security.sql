SELECT COUNT(*) FROM {OUT}.fact_trade f LEFT JOIN {OUT}.dim_security s ON s.symbol = f.symbol WHERE s.symbol IS NULL
