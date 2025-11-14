-- sql_queries/kpi_performance_comparison.sql
SELECT 
    symbol,
    COUNT(*) as total_days,
    ROUND(AVG(close)::NUMERIC, 2) AS avg_close_price,
    ROUND(MIN(low)::NUMERIC, 2) AS lowest_price,
    ROUND(MAX(high)::NUMERIC, 2) AS highest_price,
    ROUND(AVG(volume)::NUMERIC, 0) AS avg_volume
FROM stock_market
WHERE symbol IN ('AAPL', 'GOOG', 'MSFT', 'AMZN', 'NVDA')
GROUP BY symbol
ORDER BY avg_close_price DESC;