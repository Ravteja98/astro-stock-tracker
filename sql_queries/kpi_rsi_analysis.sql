-- sql_queries/kpi_rsi_analysis.sql
WITH rsi_data AS (
    SELECT
        symbol,
        TO_TIMESTAMP(timestamp) AS date_time,
        close,
        GREATEST(close - LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp), 0) AS gain,
        GREATEST(LAG(close) OVER (PARTITION BY symbol ORDER BY timestamp) - close, 0) AS loss
    FROM stock_market
    WHERE symbol IN ('AAPL', 'GOOG', 'MSFT', 'AMZN', 'NVDA')
)
-- [Full RSI calculation...]