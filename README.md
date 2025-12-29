# Sales-Demand-Forecasting-Pipeline-SQL-Python-and-ML-
This project is an end-to-end demand forecasting pipeline combining SQL-based feature engineering with Python machine learning models.

The system shows an analytics workflow where:
•	Raw transactional sales data is aggregated in PostgreSQL
•	Time-series and calendar features are engineered in SQL
•	Forecasting models are trained in Python
•	Predictions are written back into the database with version control

## Architecture
•	PostgreSQL: data storage, aggregation, feature engineering
•	Python: modeling, evaluation, forecasting
•	Models: Linear Regression, Random Forest
•	Horizon: 30-day demand forecast

## Data Pipeline
1.	Load raw sales data into PostgreSQL
2.	Aggregate daily product-level sales
3.	Engineer lag, rolling average, and calendar features in SQL
4.	Train forecasting models in Python
5.	Write forecasts back into PostgreSQL with enforced uniqueness constraints

## Key Features
•	Daily aggregation using GROUP BY
•	Time-series features (lags, rolling windows)
•	Calendar features (day-of-week, weekend flags)
•	Model versioning and primary-key enforcement
•	Idempotent database writes (safe re-runs)

## Models & Performance
•	Linear Regression (baseline)
•	Random Forest with calendar features (improved accuracy)

## Evaluation metrics:
•	MAE
•	RMSE

## Database Design
Forecasts are stored using a composite primary key: (forecast_date, product_id, location_id, model_name)

This prevents duplicate predictions and enforces data integrity.

## Technologies
•	PostgreSQL
•	Python (pandas, scikit-learn, SQLAlchemy)
•	SQL (window functions, feature engineering)
•	Jupyter Notebook
