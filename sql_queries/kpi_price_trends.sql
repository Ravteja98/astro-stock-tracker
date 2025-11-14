-- sql_queries/kpi_price_trends.sql
SELECT
    symbol,
    TO_TIMESTAMP(timestamp) AS date_time,
    close,
    ROUND(((close - open) / open * 100)::NUMERIC, 2) AS daily_change_pct
FROM stock_market
WHERE symbol IN ('AAPL', 'GOOG', 'MSFT', 'AMZN', 'NVDA')
ORDER BY date_time, symbol;