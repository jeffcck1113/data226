CREATE WAREHOUSE IF NOT EXISTS compute_wh
WITH
    WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 300        
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS dev.raw_data;
CREATE SCHEMA IF NOT EXISTS dev.analytics;
CREATE SCHEMA IF NOT EXISTS dev.adhoc;




-- tsla price table
SELECT SYMBOL AS STOCK_SYMBOL, LEFT(DATE, 10) AS DATE, OPEN, 
CLOSE, LOW AS MIN, HIGH AS MAX, VOLUME
FROM dev.raw_data.tsla_price;

-- tsm price table
SELECT SYMBOL AS STOCK_SYMBOL, LEFT(DATE, 10) AS DATE, OPEN, 
CLOSE, LOW AS MIN, HIGH AS MAX, VOLUME
FROM dev.raw_data.tsm_price;


-- final table with prediction for tsla
SELECT SYMBOL, LEFT(DATE, 10) AS DATE,
       ACTUAL, ROUND(FORECAST, 2), ROUND(LOWER_BOUND, 2),  ROUND(UPPER_BOUND, 2)
FROM dev.analytics.tsla_price_7days_prediction
ORDER BY date DESC;


-- final table with prediction for tsm
SELECT SYMBOL, LEFT(DATE, 10) AS DATE,
       ACTUAL, ROUND(FORECAST, 2), ROUND(LOWER_BOUND, 2),  ROUND(UPPER_BOUND, 2)
FROM dev.analytics.tsm_price_7days_prediction
ORDER BY date DESC;
