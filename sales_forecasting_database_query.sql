SELECT COUNT(*) FROM raw_sales;
SELECT * FROM raw_sales LIMIT 5;
SELECT * FROM raw_sales LIMIT 10;

SELECT sales_date, product_id, location_id, SUM(units_sold) AS units_sold, SUM(revenue) AS revenue
FROM raw_sales
GROUP BY sales_date, product_id, location_id
ORDER BY sales_date, product_id, location_id;


SELECT COUNT(*) FROM raw_sales;
SELECT COUNT(*) FROM (SELECT sales_date, product_id, location_id FROM raw_sales
GROUP BY sales_date, product_id, location_id);

-- Creating a Table daily_product_sales
CREATE TABLE daily_product_sales (
    sales_date DATE,
    product_id INT,
    location_id INT,
    units_sold INT,
    revenue NUMERIC(12,2),
    PRIMARY KEY (sales_date, product_id, location_id)
);


INSERT INTO daily_product_sales (
    sales_date,
    product_id,
    location_id,
    units_sold,
    revenue
)
SELECT
    sales_date,
    product_id,
    location_id,
    SUM(units_sold) AS units_sold,
    SUM(revenue) AS revenue
FROM raw_sales
GROUP BY
    sales_date,
    product_id,
    location_id;


SELECT *
FROM daily_product_sales
ORDER BY sales_date
LIMIT 10;

SELECT
    MIN(sales_date),
    MAX(sales_date),
    COUNT(*) AS total_rows
FROM daily_product_sales;

-- Time Series Feature Engineering (for each date, we want to know about the past to predict the future)
-- LAG (Give me yesterday’s value, last week’s value, etc., without collapsing rows.” This is impossible with GROUP BY. This is why window functions exist.)
SELECT
    sales_date,
    product_id,
    location_id,
    units_sold
FROM daily_product_sales
WHERE product_id = 101
  AND location_id = 1
ORDER BY sales_date
LIMIT 10;

-- Build LAG features (core skill)
SELECT
    sales_date,
    product_id,
    location_id,
    units_sold,

    LAG(units_sold, 1) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
    ) AS sales_lag_1,

    LAG(units_sold, 7) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
    ) AS sales_lag_7

FROM daily_product_sales
ORDER BY product_id, location_id, sales_date;

-- ROLLING AVERAGES (Smoothing noise) - daily sales are noisy and rolling averages show underlying demand
-- 7-day and 30-day rolling averages
SELECT
    sales_date,
    product_id,
    location_id,
    units_sold,

    AVG(units_sold) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7,

    AVG(units_sold) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_30

FROM daily_product_sales
ORDER BY product_id, location_id, sales_date;

-- TREND SIGNALS (Direction of Demand)
-- Simple trend signal (difference from past)
SELECT
    sales_date,
    product_id,
    location_id,
    units_sold,

    units_sold
      - LAG(units_sold, 7) OVER (
            PARTITION BY product_id, location_id
            ORDER BY sales_date
        ) AS weekly_trend_change

FROM daily_product_sales
ORDER BY product_id, location_id, sales_date;

-- PART D — Create the FEATURE TABLE
CREATE TABLE product_sales_features AS
SELECT
    sales_date AS feature_date,
    product_id,
    location_id,
    units_sold,

    LAG(units_sold, 1) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
    ) AS sales_lag_1,

    LAG(units_sold, 7) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
    ) AS sales_lag_7,

    AVG(units_sold) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7,

    AVG(units_sold) OVER (
        PARTITION BY product_id, location_id
        ORDER BY sales_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_30

FROM daily_product_sales;

-- verifying the table
SELECT *
FROM product_sales_features
ORDER BY product_id, location_id, feature_date
LIMIT 10;


-- CREATING FORECAST TABLE TO COMPLETE END-END FORECASTING SYSTEM
CREATE TABLE IF NOT EXISTS demand_forecasts (
    forecast_date DATE,
    product_id INT,
    location_id INT,
    predicted_units NUMERIC(10,2),
    model_name TEXT,
    model_version TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (forecast_date, product_id, location_id, model_name)
);

-- Deleting Existing Rows due to Rerun
DELETE FROM demand_forecasts
WHERE model_name = 'linear_regression'
  AND model_version = 'v1';


-- VERIFYING THE SYSTEM
SELECT *
FROM demand_forecasts
ORDER BY created_at DESC
LIMIT 10;

-- CREATE AN ENHANCED FEATURE TABLE
DROP TABLE IF EXISTS product_sales_features_v2;

CREATE TABLE product_sales_features_v2 AS
SELECT
    d.sales_date AS feature_date,
    d.product_id,
    d.location_id,
    d.units_sold,

    LAG(d.units_sold, 1) OVER (
        PARTITION BY d.product_id, d.location_id
        ORDER BY d.sales_date
    ) AS sales_lag_1,

    LAG(d.units_sold, 7) OVER (
        PARTITION BY d.product_id, d.location_id
        ORDER BY d.sales_date
    ) AS sales_lag_7,

    AVG(d.units_sold) OVER (
        PARTITION BY d.product_id, d.location_id
        ORDER BY d.sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7,

    AVG(d.units_sold) OVER (
        PARTITION BY d.product_id, d.location_id
        ORDER BY d.sales_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_30,

    EXTRACT(DOW FROM d.sales_date) AS dow,

    CASE
        WHEN EXTRACT(DOW FROM d.sales_date) IN (0,6) THEN 1
        ELSE 0
    END AS is_weekend,

    EXTRACT(MONTH FROM d.sales_date) AS month

FROM daily_product_sales d;


-- Verifying the new feature table
SELECT *
FROM product_sales_features_v2
ORDER BY feature_date
LIMIT 10;

-- v2 (RF and Calendar) 30-Day Forecasts into PostgreSQL
CREATE TABLE IF NOT EXISTS demand_forecasts (
    forecast_date DATE,
    product_id INT,
    location_id INT,
    predicted_units NUMERIC(10,2),
    model_name TEXT,
    model_version TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (forecast_date, product_id, location_id, model_name)
);

-- checking if rows exist
SELECT COUNT(*)
FROM demand_forecasts
WHERE model_version = 'rf_calendar_v2';


SELECT
    forecast_date,
    product_id,
    location_id,
    predicted_units,
    model_name,
    model_version,
    created_at
FROM demand_forecasts
WHERE model_version = 'rf_calendar_v2'
ORDER BY forecast_date;

-- verifying machine learning model
SELECT
    model_name,
    model_version,
    COUNT(*) AS rows
FROM demand_forecasts
GROUP BY model_name, model_version;




