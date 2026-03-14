-- LESSON 5: Feature Engineering for Forecasting

USE ROLE SYSADMIN;
USE WAREHOUSE ML_WH;

-- Create a New Training Data View and Add a Day of Week Column:
create or replace view camillas_db.forecasting.train_2_model_practice_data(
	practice_date,
	day_of_week,
	goals_attempted,
	goals_scored
) as
select 
    practice_date,
    dayname(practice_date) as day_of_week,
    goals_attempted,
    goals_scored
from camillas_db.forecasting.practice_stats
    where practice_date < '2025-07-01';

-- Using the same update pattern, create the VALIDATE_2_MODEL_PRACTICE_DATA view:
create or replace view camillas_db.forecasting.validate_2_model_practice_data(
	practice_date,
	day_of_week,
	goals_attempted,
	goals_scored
) as
select 
    practice_date,
    dayname(practice_date) as day_of_week,
    goals_attempted,
    goals_scored
from camillas_db.forecasting.practice_stats
    where practice_date >= '2025-07-01';


-- Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role SYSADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema FORECASTING;

-- Inspect the first 10 rows of training data. This is the data we'll use to create the model:
select * from TRAIN_2_MODEL_PRACTICE_DATA limit 10;

-- Prepare training data. Timestamp_ntz is a required format:
CREATE VIEW TRAIN_2_MODEL_PRACTICE_DATA_v1 AS SELECT
    * EXCLUDE PRACTICE_DATE,
    to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
FROM TRAIN_2_MODEL_PRACTICE_DATA;

-- Prepare prediction data. Timestamp_ntz is a required format:
CREATE VIEW VALIDATE_2_MODEL_PRACTICE_DATA_v1 AS SELECT
    * EXCLUDE PRACTICE_DATE,
    to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
FROM VALIDATE_2_MODEL_PRACTICE_DATA;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create the model:
CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_4cast_w_dayofweek(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'TRAIN_2_MODEL_PRACTICE_DATA_v1'),
    SERIES_COLNAME => 'DAY_OF_WEEK',
    TIMESTAMP_COLNAME => 'PRACTICE_DATE_v1',
    TARGET_COLNAME => 'GOALS_SCORED',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions and store the results to a table:
BEGIN
    -- Predictions creation:
    CALL camillas_practice_goal_4cast_w_dayofweek!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VALIDATE_2_MODEL_PRACTICE_DATA_v1'),
        SERIES_COLNAME => 'GOALS_SCORED',
        TIMESTAMP_COLNAME => 'PRACTICE_DATE_v1',
        -- Set prediction interval:
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- Store predictions to a table:
    LET x := SQLID;
    CREATE TABLE second_goals_forecast AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View predictions:
SELECT * FROM second_goals_forecast;

-- Union predictions with historical data, then view the results in a chart:
SELECT DAY_OF_WEEK, PRACTICE_DATE, GOALS_SCORED AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM TRAIN_2_MODEL_PRACTICE_DATA
UNION ALL
SELECT replace(series, '"', '') as DAY_OF_WEEK, ts as PRACTICE_DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM second_goals_forecast;

-- Special Union Select Statement to Compare the Difference Day of Week Made in the Predictions:
select practice_date, goals_scored as actual, null as forecast_1, NULL as forecast_2
    from train_2_model_practice_data
UNION ALL
select ts as practice_date, NULL as actual, forecast as forecast_1, NULL as forecast_2
    from first_goals_forecast
UNION ALL    
select ts as practice_date, NULL as actual, null as forecast_1, forecast as forecast_2
    from second_goals_forecast;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of the model:
CALL camillas_practice_goal_4cast_w_dayofweek!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of the features, including auto-generated features:
CALL camillas_practice_goal_4cast_w_dayofweek!EXPLAIN_FEATURE_IMPORTANCE();

-------------------------------------------------------------------------------
-- Check for Lesson completion.
USE ROLE SYSADMIN;
USE SCHEMA UTIL_DB.PUBLIC;

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DSCW04' as step 
   ,( select  round(count(*)/iff(count(*)=0,1,count(*)),0) as tally
      from snowflake.account_usage.query_history
      where query_text like '%CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_4cast%'
      and execution_status = 'SUCCESS'
     ) as actual 
   , 1 as expected 
   ,'Improved Forecast Model' as description
); 