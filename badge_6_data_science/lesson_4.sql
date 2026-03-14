-- LESSON 4: Build a Model for Forcasting

/*
Tasks:

Add Another Warehouse and Schema for Camilla.

- Use the SYSADMIN role for all of these tasks. 
- Create a new schema called FORECASTING in Camilla's Database.
- Create an eXtra-Small Warehouse and call it ML_WH.
*/

-- Use the SYSADMIN role for all of these tasks:
USE ROLE SYSADMIN;

-- Create a new schema called FORECASTING in Camilla's Database:
CREATE SCHEMA IF NOT EXISTS CAMILLAS_DB.FORECASTING;

-- Create an eXtra-Small Warehouse and call it ML_WH:
CREATE OR REPLACE WAREHOUSE ML_WH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD';

-- Create a Table to Hold Camilla's Practice Session Observations:
create or replace table camillas_db.forecasting.practice_stats (
	practice_date timestamp_ntz,
	goals_scored number,
	goals_attempted number
);

-- Upload Camilla's Data:
INSERT INTO camillas_db.forecasting.practice_stats(practice_date, goals_scored, goals_attempted)
VALUES
('2025-03-30 00:00:00.000',0,0),
('2025-03-31 00:00:00.000',0,15),
('2025-04-01 00:00:00.000',0,21),
('2025-04-02 00:00:00.000',0,23),
('2025-04-03 00:00:00.000',0,24),
('2025-04-04 00:00:00.000',0,27),
('2025-04-05 00:00:00.000',0,0),
('2025-04-06 00:00:00.000',0,0),
('2025-04-07 00:00:00.000',0,15),
('2025-04-08 00:00:00.000',1,20),
('2025-04-09 00:00:00.000',1,22),
('2025-04-10 00:00:00.000',1,24),
('2025-04-11 00:00:00.000',0,30),
('2025-04-12 00:00:00.000',0,0),
('2025-04-13 00:00:00.000',0,0),
('2025-04-14 00:00:00.000',0,16),
('2025-04-15 00:00:00.000',1,18),
('2025-04-16 00:00:00.000',1,19),
('2025-04-17 00:00:00.000',1,17),
('2025-04-18 00:00:00.000',0,30),
('2025-04-19 00:00:00.000',0,0),
('2025-04-20 00:00:00.000',0,0),
('2025-04-21 00:00:00.000',1,15),
('2025-04-22 00:00:00.000',2,16),
('2025-04-23 00:00:00.000',2,17),
('2025-04-24 00:00:00.000',2,18),
('2025-04-25 00:00:00.000',1,26),
('2025-04-26 00:00:00.000',0,0),
('2025-04-27 00:00:00.000',0,0),
('2025-04-28 00:00:00.000',1,11),
('2025-04-29 00:00:00.000',2,13),
('2025-04-30 00:00:00.000',2,14),
('2025-05-01 00:00:00.000',3,15),
('2025-05-02 00:00:00.000',2,20),
('2025-05-03 00:00:00.000',0,0),
('2025-05-04 00:00:00.000',0,0),
('2025-05-05 00:00:00.000',2,9),
('2025-05-06 00:00:00.000',3,11),
('2025-05-07 00:00:00.000',3,13),
('2025-05-08 00:00:00.000',3,12),
('2025-05-09 00:00:00.000',2,15),
('2025-05-10 00:00:00.000',0,0),
('2025-05-11 00:00:00.000',0,0),
('2025-05-12 00:00:00.000',2,8),
('2025-05-13 00:00:00.000',3,9),
('2025-05-14 00:00:00.000',3,10),
('2025-05-15 00:00:00.000',3,12),
('2025-05-16 00:00:00.000',2,15),
('2025-05-17 00:00:00.000',0,0),
('2025-05-18 00:00:00.000',0,0),
('2025-05-19 00:00:00.000',3,7),
('2025-05-20 00:00:00.000',4,9),
('2025-05-21 00:00:00.000',4,9),
('2025-05-22 00:00:00.000',4,8),
('2025-05-23 00:00:00.000',3,10),
('2025-05-24 00:00:00.000',0,0),
('2025-05-25 00:00:00.000',0,0),
('2025-05-26 00:00:00.000',3,6),
('2025-05-27 00:00:00.000',4,6),
('2025-05-28 00:00:00.000',4,7),
('2025-05-29 00:00:00.000',4,7),
('2025-05-30 00:00:00.000',3,9),
('2025-05-31 00:00:00.000',0,0),
('2025-06-01 00:00:00.000',0,0),
('2025-06-02 00:00:00.000',2,5),
('2025-06-03 00:00:00.000',5,7),
('2025-06-04 00:00:00.000',6,7),
('2025-06-05 00:00:00.000',5,5),
('2025-06-06 00:00:00.000',4,9),
('2025-06-07 00:00:00.000',0,0),
('2025-06-08 00:00:00.000',0,0),
('2025-06-09 00:00:00.000',4,6),
('2025-06-10 00:00:00.000',5,7),
('2025-06-11 00:00:00.000',5,7),
('2025-06-12 00:00:00.000',5,8),
('2025-06-13 00:00:00.000',4,8),
('2025-06-14 00:00:00.000',0,0),
('2025-06-15 00:00:00.000',0,0),
('2025-06-16 00:00:00.000',4,6),
('2025-06-17 00:00:00.000',6,8),
('2025-06-18 00:00:00.000',6,8),
('2025-06-19 00:00:00.000',0,0),
('2025-06-20 00:00:00.000',5,10),
('2025-06-21 00:00:00.000',0,0),
('2025-06-22 00:00:00.000',0,0),
('2025-06-23 00:00:00.000',5,6),
('2025-06-24 00:00:00.000',6,8),
('2025-06-25 00:00:00.000',6,8),
('2025-06-26 00:00:00.000',7,9),
('2025-06-27 00:00:00.000',5,10),
('2025-06-28 00:00:00.000',0,0),
('2025-06-29 00:00:00.000',0,0),
('2025-06-30 00:00:00.000',6,7),
('2025-07-01 00:00:00.000',7,8),
('2025-07-02 00:00:00.000',7,9),
('2025-07-03 00:00:00.000',5,12),
('2025-07-04 00:00:00.000',0,0),
('2025-07-05 00:00:00.000',0,0),
('2025-07-06 00:00:00.000',0,0),
('2025-07-07 00:00:00.000',7,7),
('2025-07-08 00:00:00.000',8,9),
('2025-07-09 00:00:00.000',8,9),
('2025-07-10 00:00:00.000',9,9),
('2025-07-11 00:00:00.000',5,9),
('2025-07-12 00:00:00.000',0,0),
('2025-07-13 00:00:00.000',0,0),
('2025-07-14 00:00:00.000',7,8),
('2025-07-15 00:00:00.000',8,9),
('2025-07-16 00:00:00.000',9,10),
('2025-07-17 00:00:00.000',9,10),
('2025-07-18 00:00:00.000',5,11),
('2025-07-19 00:00:00.000',0,0),
('2025-07-20 00:00:00.000',0,0),
('2025-07-21 00:00:00.000',8,9),
('2025-07-22 00:00:00.000',9,10),
('2025-07-23 00:00:00.000',9,10),
('2025-07-24 00:00:00.000',9,10),
('2025-07-25 00:00:00.000',5,11),
('2025-07-26 00:00:00.000',0,0),
('2025-07-27 00:00:00.000',0,0),
('2025-07-28 00:00:00.000',8,9),
('2025-07-29 00:00:00.000',9,9),
('2025-07-30 00:00:00.000',9,10),
('2025-07-31 00:00:00.000',9,10),
('2025-08-01 00:00:00.000',6,11),
('2025-08-02 00:00:00.000',0,0),
('2025-08-03 00:00:00.000',0,0),
('2025-08-04 00:00:00.000',5,8),
('2025-08-05 00:00:00.000',9,9),
('2025-08-06 00:00:00.000',9,10),
('2025-08-07 00:00:00.000',9,9),
('2025-08-08 00:00:00.000',6,11),
('2025-08-09 00:00:00.000',0,0),
('2025-08-10 00:00:00.000',0,0),
('2025-08-11 00:00:00.000',7,7),
('2025-08-12 00:00:00.000',8,9),
('2025-08-13 00:00:00.000',8,9),
('2025-08-14 00:00:00.000',9,10),
('2025-08-15 00:00:00.000',5,10),
('2025-08-16 00:00:00.000',0,0),
('2025-08-17 00:00:00.000',0,0),
('2025-08-18 00:00:00.000',5,7),
('2025-08-19 00:00:00.000',8,9),
('2025-08-20 00:00:00.000',9,11),
('2025-08-21 00:00:00.000',7,8),
('2025-08-22 00:00:00.000',4,10),
('2025-08-23 00:00:00.000',0,0),
('2025-08-24 00:00:00.000',0,0),
('2025-08-25 00:00:00.000',3,7),
('2025-08-26 00:00:00.000',7,9),
('2025-08-27 00:00:00.000',8,9),
('2025-08-28 00:00:00.000',8,9),
('2025-08-29 00:00:00.000',5,12),
('2025-08-30 00:00:00.000',0,0),
('2025-08-31 00:00:00.000',0,0);

/*
Preparing Data for Forecasting

To forecast how the players will perform at future practices, we want to begin by
feeding Snowflake some historical data. But, we don't want to give it ALL the historical
data. Want to hold back some of that data so that we can use it to validate our forecast!

We need to use MOST of our data to TRAIN the forecasting model. Then use the REMAINING
data to VALIDATE or EVALUATE the model that we get back. 

To divide the data easily, we'll just create two views. 
*/

-- Create Views for Model Training and then Validation:
create or replace view camillas_db.forecasting.train_model_practice_data(
	  practice_date,
	  goals_attempted,
	  goals_scored
) as
  select 
    practice_date, 
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date < '2025-07-01';


-- Make a view that uses data from july forward for validating the model
create or replace view camillas_db.forecasting.validate_model_practice_data(
	   practice_date,
	   goals_attempted,
	   goals_scored
) as
  select 
    practice_date, 
    goals_attempted,
    goals_scored
  from camillas_db.forecasting.practice_stats
  where practice_date >= '2025-07-01';


-- Cortex Project:
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role SYSADMIN;
use warehouse ML_WH;
use database CAMILLAS_DB;
use schema FORECASTING;

-- Inspect the first 10 rows of training data. This is the data will be used to create model:
select * from TRAIN_MODEL_PRACTICE_DATA limit 10;

-- Prepare training data. Timestamp_ntz is a required format:
CREATE VIEW TRAIN_MODEL_PRACTICE_DATA_v1 AS SELECT
    * EXCLUDE PRACTICE_DATE,
    to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
FROM TRAIN_MODEL_PRACTICE_DATA;

-- Prepare prediction data. Timestamp_ntz is a required format:
CREATE VIEW VALIDATE_MODEL_PRACTICE_DATA_v1 AS SELECT
    * EXCLUDE PRACTICE_DATE,
    to_timestamp_ntz(PRACTICE_DATE) as PRACTICE_DATE_v1
FROM VALIDATE_MODEL_PRACTICE_DATA;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create the model:
CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_forecasting(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'TRAIN_MODEL_PRACTICE_DATA_v1'),
    TIMESTAMP_COLNAME => 'PRACTICE_DATE_v1',
    TARGET_COLNAME => 'GOALS_SCORED'
);

-- Generate predictions and store the results to a table:
BEGIN
    -- Predictions creation:
    CALL camillas_practice_goal_forecasting!FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'VALIDATE_MODEL_PRACTICE_DATA_v1'),
        TIMESTAMP_COLNAME => 'PRACTICE_DATE_v1',
        -- Set prediction interval:
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- Store predictions to a table:
    LET x := SQLID;
    CREATE TABLE first_goals_forecast AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View predictions:
SELECT * FROM first_goals_forecast;

-- Union predictions with historical data, then view the results in a chart:
SELECT PRACTICE_DATE, GOALS_SCORED AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM TRAIN_MODEL_PRACTICE_DATA
UNION ALL
SELECT ts as PRACTICE_DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM first_goals_forecast;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of the model:
CALL camillas_practice_goal_forecasting!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of the features, including auto-generated features:
CALL camillas_practice_goal_forecasting!EXPLAIN_FEATURE_IMPORTANCE();


-------------------------------------------------------------------------------
-- Check for Lesson completion.
USE ROLE SYSADMIN;
USE SCHEMA UTIL_DB.PUBLIC;

-- Set your worksheet drop lists
-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DSCW03' as step 
   ,( select  round(count(*)/iff(count(*)=0,1,count(*)),0) as tally
      from snowflake.account_usage.query_history
      where query_text like '%CREATE SNOWFLAKE.ML.FORECAST camillas_practice_goal_forecasting%'
      and execution_status = 'SUCCESS'
     ) as actual 
   , 1 as expected 
   ,'Created Forecast Model' as description
); 