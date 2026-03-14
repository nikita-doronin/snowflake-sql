-- LESSON 3: Cortex Analyst

/*
Tasks:

- Use the SYSADMIN role for all of these tasks. 
- Create a database called CAMILLAS_DB.
- Drop the PUBLIC schema and create a new schema called CORTEX_ANALYST.
- Create a Snowflake Managed Stage in the CORTEX_ANALYST Schema and name it CORTEX_ANALYST_MODEL_STAGE. Use client-side encryption. 
- Create an eXtra-Small Warehouse and call it LLM_WH
*/

-- Use the SYSADMIN role for all of these tasks.:
USE ROLE SYSADMIN;

-- Create a database called CAMILLAS_DB:
CREATE DATABASE IF NOT EXISTS CAMILLAS_DB;

-- Drop the PUBLIC schema and create a new schema called CORTEX_ANALYST:
DROP SCHEMA IF EXISTS CAMILLAS_DB.PUBLIC;
CREATE OR REPLACE SCHEMA CAMILLAS_DB.CORTEX_ANALYST;

-- Create a Snowflake Managed Stage in the CORTEX_ANALYST Schema and name it CORTEX_ANALYST_MODEL_STAGE. Use client-side encryption:
-- Internal Stage:
CREATE OR REPLACE STAGE CAMILLAS_DB.CORTEX_ANALYST.CORTEX_ANALYST_MODEL_STAGE;

-- Enable Directory for the stage to use it in CORTEX ANALYST:
ALTER STAGE CAMILLAS_DB.CORTEX_ANALYST.CORTEX_ANALYST_MODEL_STAGE
SET DIRECTORY = (ENABLE = TRUE);

-- Create an eXtra-Small Warehouse and call it LLM_WH:
CREATE OR REPLACE WAREHOUSE LLM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD';

-- Create and Populate a Teams Table:
create or replace table camillas_db.cortex_analyst.camillas_teams
( 
    team_id number,
    team_name varchar(50),
    kit_color varchar(20),
    coach varchar(100),
    emoji_symbol varchar(5)
);

insert into camillas_db.cortex_analyst.camillas_teams
values
(1,'Blue Sky Strikers','cerulean','Stormy McLeod', '💙☁️⚡️'),
(2,'Pitch Blazing Bombers','emerald','Kelly Groen','🌱🔥💣' ),
(3,'Solar Flashing Flares','marigold','Ravi Bahsin', '☀️🔥'),
(4,'Terracotta Tirade','terracotta','Clay Skála', '🪴💪');

-- Add a Table for Tournament Match Locations:
create or replace table camillas_db.cortex_analyst.match_locations
(
 location_id number, 
 location_name varchar(50)
);

insert into camillas_db.cortex_analyst.match_locations
values 
(1, 'Main Street Park - Pitch 1'),
(2, 'Main Street Park - Pitch 2'),
(3, 'Central Park - North Pitch'),
(4, 'Central Park - South Pitch');

-- Add a Table for the Tournament Schedule:
create or replace table camillas_db.cortex_analyst.match_schedule
( 
    home_team_id number,
    away_team_id number,
    location_id number,
    match_datetime timestamp_ntz  
);

insert into camillas_db.cortex_analyst.match_schedule
values
(1,2,1,'2025-06-07 08:00:00'),
(3,4,2,'2025-06-07 08:00:00'),
(2,3,3,'2025-06-07 12:00:00'),
(1,4,4,'2025-06-07 12:00:00'),
(1,3,1,'2025-06-07 16:00:00'),
(2,4,2,'2025-06-07 16:00:00')
;

-------------------------------------------------------------------------------
-- Check for Lesson completion.
USE ROLE SYSADMIN;
USE SCHEMA UTIL_DB.PUBLIC;

-- Set your worksheet drop lists
--This DORA Check Requires that you RUN two Statements, one right after the other
list @camillas_db.cortex_analyst.cortex_analyst_model_stage;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the LIST command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DSCW02' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "name" = 'cortex_analyst_model_stage/CAMILLAS_JUNE_TOURNAMENT.yaml') as actual
 , 1 as expected
 ,'Semantic Model Complete' as description
); 