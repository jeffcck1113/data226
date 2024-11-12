SELECT 
    date,
    symbol,
    close,
    AVG(close) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_days
FROM {{ source('raw_data', 'nvda_price') }}
ORDER BY date DESC
