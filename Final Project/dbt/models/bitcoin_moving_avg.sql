SELECT
    symbol,
    TIME,
    PRICE_CLOSE,
    AVG(PRICE_CLOSE) OVER (
        ORDER BY TIME 
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS moving_average_60_mins,
    AVG(PRICE_CLOSE) OVER (
        ORDER BY TIME 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS moving_average_5_mins
FROM {{ source('raw_data', 'bitcoin_price') }}
ORDER BY TIME DESC