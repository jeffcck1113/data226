SELECT
    symbol,
    TIME,
    PRICE_CLOSE,
    AVG(PRICE_CLOSE) OVER (
        ORDER BY TIME 
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) AS moving_average_25_mins
FROM {{ source('raw_data', 'bitcoin_price') }}
ORDER BY TIME DESC