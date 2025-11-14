-- sql_queries/kpi_volume_analysis.sql
SELECT 
    TO_TIMESTAMP(timestamp) as date_time,
    symbol,
    volume,
    high,
    low,
    close,
    ROUND(((close - open) / open * 100)::NUMERIC, 2) AS price_change_pct
FROM stock_market
WHERE symbol IN ('AAPL', 'GOOG', 'MSFT', 'AMZN', 'NVDA')
ORDER BY volume DESC;