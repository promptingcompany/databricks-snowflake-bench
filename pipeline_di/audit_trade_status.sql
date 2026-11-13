SELECT COUNT(*) FROM {OUT}.fact_trade WHERE status NOT IN ('PNDG', 'CMPT', 'CNCL')
