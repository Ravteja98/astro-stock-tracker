-- sql_queries/kpi_volatility_analysis.sql
SELECT
    symbol,
    ROUND(STDDEV(close)::NUMERIC, 2) AS price_volatility,
    ROUND(AVG((high - low) / low * 100)::NUMERIC, 2) AS avg_daily_volatility_pct
FROM stock_market
WHERE symbol IN ('AAPL', 'GOOG', 'MSFT', 'AMZN', 'NVDA')
GROUP BY symbol
ORDER BY price_volatility DESC;