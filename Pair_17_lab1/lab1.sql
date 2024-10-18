CREATE WAREHOUSE IF NOT EXISTS compute_wh
WITH
    WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 300        
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS dev.raw_data;
CREATE SCHEMA IF NOT EXISTS dev.analytics;
CREATE SCHEMA IF NOT EXISTS dev.adhoc;





SELECT SYMBOL AS STOCK_SYMBOL, LEFT(DATE, 10) AS DATE, OPEN, 
CLOSE, LOW AS MIN, HIGH AS MAX, VOLUME
FROM dev.raw_data.tsla_price;

SELECT SYMBOL AS STOCK_SYMBOL, LEFT(DATE, 10) AS DATE, OPEN, 
CLOSE, LOW AS MIN, HIGH AS MAX, VOLUME
FROM dev.raw_data.tsm_price;



SELECT SYMBOL, LEFT(DATE, 10) AS DATE,
       ACTUAL, ROUND(FORECAST, 2) AS FORECAST, 
       ROUND(LOWER_BOUND, 2) AS LOWER_BOUND,  ROUND(UPPER_BOUND, 2) AS UPPER_BOUND
FROM dev.analytics.tsla_price_7days_prediction
ORDER BY date DESC;



SELECT SYMBOL, LEFT(DATE, 10) AS DATE,
       ACTUAL, ROUND(FORECAST, 2) AS FORECAST,
       ROUND(LOWER_BOUND, 2) AS LOWER_BOUND,  ROUND(UPPER_BOUND, 2) AS UPPER_BOUND
FROM dev.analytics.tsm_price_7days_prediction
ORDER BY date DESC;




