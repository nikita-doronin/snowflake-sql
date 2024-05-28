-- LESSON 2 - Project Kick-Off and Database Set Up
ALTER USER <my_user_name> SET DEFAULT_ROLE = 'SYSADMIN';

CREATE TABLE GAME_LOGS 
    (
    RAW_LOG VARIANT
    );

-- Test the Stage:
list @uni_kishore/kickoff;

-- Challenge Lab: Create a File Format
-- Use SYSADMIN.
-- Create a File Format in the AGS_GAME_AUDIENCE.RAW schema named FF_JSON_LOGS.
-- Set the data file Type to JSON 
-- Set the Strip Outer Array Property to TRUE
-- Solution below:
USE ROLE SYSADMIN;

CREATE OR REPLACE FILE FORMAT FF_JSON_LOGS
    TYPE = JSON,
    strip_outer_array = TRUE;

-- Exploring the File Before Loading It:
select $1 from @uni_kishore/kickoff
(file_format => ff_json_logs);

-- Load the File Into The Table:
copy into ags_game_audience.raw.GAME_LOGS
from @uni_kishore/kickoff
file_format = (format_name=FF_JSON_LOGS);

-- Build a Select Statement that Separates Every Attribute into It's Own Column:
select
    RAW_LOG: agent:: text as AGENT,
    RAW_LOG: datetime_iso8601:: TIMESTAMP_NTZ as datetime_iso8601,
    RAW_LOG: user_event:: text as USER_EVENT,
    RAW_LOG: user_login:: text as USER_LOGIN,
    RAW_LOG
from game_logs;

-- Challenge Lab: Create Your View
-- Use SYSADMIN.
-- Create a view named LOGS in the RAW schema.
-- Solution below:
USE ROLE SYSADMIN;

CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.RAW.LOGS AS 
    (SELECT
    RAW_LOG: agent:: text AS AGENT,
    RAW_LOG: datetime_iso8601:: TIMESTAMP_NTZ AS datetime_iso8601,
    RAW_LOG: user_event:: text AS USER_EVENT,
    RAW_LOG: user_login:: text AS USER_LOGIN,
    RAW_LOG
FROM game_logs);

SELECT * FROM AGS_GAME_AUDIENCE.RAW.LOGS;

-- LESSON 3: Time Zones, Dates and Timestamps

-- Change the Time Zone:
-- Current time zone in account(and/or session):
select current_timestamp();

-- Different settions:
alter session set timezone = 'UTC';
select current_timestamp();

alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

-- Show the account parameter called timezone:
show parameters like 'timezone';

-- CHALLENGE Lab: Update Your Process to Accommodate the New File
-- Find the new file Agnie downloaded from the game platform by listing files in 
-- the stage you already set up. Agnie put it in a different folder. It's not in the "kickoff" folder. 
-- Assess whether the GAME_LOGS table will need to be modified to accommodate the added IP_ADDRESS field. 
-- If GAME_LOGS table needs to be changed, change it. 
-- Load the file into the GAME_LOGS table.  To do this, you can likely make one adjustment to the 
-- COPY INTO command you ran earlier. 
-- Solution below:
select $1 from @uni_kishore/updated_feed
(file_format => ff_json_logs);

copy into ags_game_audience.raw.GAME_LOGS
from @uni_kishore/updated_feed
file_format = (format_name=FF_JSON_LOGS);

-- Filter Out the Old Records:
-- 1st option, Looking for empty AGENT column:
select * 
from ags_game_audience.raw.LOGS
where agent is null;

-- 2nd option, Looking for non-empty IP_ADDRESS column:
select 
    RAW_LOG:ip_address::text as IP_ADDRESS,
    *
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

-- CHALLENGE Lab: Update Your LOGS View
-- Change the LOGS view definition so that it no longer contains an AGENT column.
-- (Instead of create view, you will need create or replace view)
-- Change the LOGS view definition so that it now contains the IP_ADDRESS column.
-- Add a WHERE clause that will exclude the first set of records from the view results.
-- Do NOT remove the rows from the table.
-- Solution below:
USE ROLE SYSADMIN;

CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.RAW.LOGS AS 
    (
    SELECT
        RAW_LOG: ip_address:: text AS IP_ADDRESS,
        RAW_LOG: user_event:: text AS USER_EVENT,
        RAW_LOG: user_login:: text AS USER_LOGIN,
        RAW_LOG: datetime_iso8601:: TIMESTAMP_NTZ AS datetime_iso8601,
        RAW_LOG
    FROM game_logs
    WHERE IP_ADDRESS IS NOT NULL
    );

SELECT * FROM AGS_GAME_AUDIENCE.RAW.LOGS;

-- LESSON 4: Extracting, Transforming, and Loading
-- Pull Out PARSE_IP Results Fields:
select parse_ip('107.217.231.17','inet'):host;
select parse_ip('107.217.231.17','inet'):family;

-- Look up Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function:
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

-- Join the log and location tables to add time zone to each row using the PARSE_IP function:
select 
    logs.*,
    loc.city,
    loc.region,
    loc.country,
    loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

-- Use two functions supplied by IPShare to help with an efficient IP Lookup Process:
SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_event,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

-- Challenge Lab: Add a Local Time Zone Column to Your Select
-- Add a column called GAME_EVENT_LTZ to the last code block you ran.
-- After you create the new column, use the test rows created by Kishore's 
-- sister to make sure the conversion worked.
-- Solution below:
SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_event,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    CONVERT_TIMEZONE('UTC',timezone,logs.datetime_iso8601) AS GAME_EVENT_LTZ
FROM AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


-- Using labels like "Early morning" and "Mid-morning"...:
-- A Look Up table to convert from hour number to "time of day name":
create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

-- Insert statement to add all 24 rows to the table:
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

-- Check to table to see if data loaded properly:
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;

-- Join the log and location tables to add time zone to each row 
-- using the PARSE_IP function and time_of_day_lu table:
SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_event,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    CONVERT_TIMEZONE('UTC',timezone,logs.datetime_iso8601) AS GAME_EVENT_LTZ,
    dayname(game_event_ltz) AS DOW_NAME,
    -- DATE_PART(hour, game_event_ltz) AS is_this_how_to_get_an_hour,
    TOD_NAME
FROM AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.time_of_day_lu lu
on DATE_PART(hour, game_event_ltz) = lu.hour;

-- Convert the code above into a new table:
CREATE OR REPLACE TABLE ags_game_audience.enhanced.logs_enhanced AS
    (
    SELECT
        logs.ip_address,
        logs.user_login AS GAMER_NAME,
        logs.user_event AS GAME_EVENT_NAME,
        logs.datetime_iso8601 AS GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone AS GAMER_LTZ_NAME,
        CONVERT_TIMEZONE('UTC',timezone,logs.datetime_iso8601) AS GAME_EVENT_LTZ,
        dayname(game_event_ltz) AS DOW_NAME,
        -- DATE_PART(hour, game_event_ltz) AS is_this_how_to_get_an_hour,
        TOD_NAME
    FROM AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.time_of_day_lu lu
    ON DATE_PART(hour, game_event_ltz) = lu.hour
    );

-- LESSON 5: Productionizing Our Work

use role accountadmin;
-- Test tasks while in SYSADMIN role:
grant execute task on account to role SYSADMIN;

use role sysadmin; 

-- Run the task:
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- The SHOW command might come in handy to look at the task:
show tasks in account;

-- Look at any task more in depth using DESCRIBE:
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Execute the Task a Few More Times:
-- Run the task a few times to see changes in the RUN HISTORY
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Executing the Task to TRY to Load More Rows:
-- Make a note of how many rows you have in the table:
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Run the task to load more rows:
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Check to see how many rows were added (if any):
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- First dump all the rows out of the table:
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Then put them all back in:
INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
    (
    SELECT
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_ltz) as DOW_NAME,
        TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour
    );

/*Snowflake has the ability to CLONE databases, schemas, tables and more which means
this sort of switching out can be done very easily but this isn't really a version of
the old-school rebuild and replace. It's pretty significantly different. That's okay,
because almost no orgs rebuild their warehouses from scratch each night anymore.*/

-- Create a Backup Copy of the Table
-- Clone the table to save this version as a backup
-- Since it holds the records from the UPDATED FEED file, name it _UFL:
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF
clone ags_game_audience.enhanced.LOGS_ENHANCED;

-- Adding the datetime field was enough to remove the duplicate errors,
-- but we like to be extra safe, so we also added the event name also (e.g. 'login' and 'logoff'):
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING AGS_GAME_AUDIENCE.RAW.LOGS r
ON r.user_login = e.GAMER_NAME
AND r.datetime_iso8601 = e.GAME_EVENT_UTC
AND r.user_event = e.GAME_EVENT_NAME
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey, I updated matching rows!';

-- Truncate Again for a Fresh Start:
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Build Insert Merge and replace the old task with this new version:
CREATE OR REPLACE task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse=COMPUTE_WH
    schedule='5 minute'
    AS
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING
    (
    SELECT
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        CONVERT_TIMEZONE( 'UTC', timezone, logs.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_Ltz) as DOW_NAME,
        TOD_NAME
        from AGS_GAME_AUDIENCE.RAW.LOGS logs
        JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
    ) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name

WHEN NOT MATCHED THEN
INSERT 
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    )
VALUES
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    );

-- Execute the task to check that it succeeds:
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--------------------------------------------------------------------------------------
-- Testing cycle for MERGE. Use these commands to make sure the Merge works as expected:
-- Write down the number of records in table:
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Run the Merge a few times. No new rows should be added at this time:
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Check to see if row count changed:
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Insert a test record into Raw Table 
-- Change the user_event field each time to create "new" records 
-- editing the ip_address or datetime_iso8601 can complicate things more than they need to 
-- editing the user_login will make it harder to remove the fake records after finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

-- After inserting a new row, run the Merge again:
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Check to see if any rows were added:
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- When we are confident that the merge is working, delete the raw records:
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

-- Delete the fake rows from the enhanced table:
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

-- Row count should be back to what it was in the beginning:
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
--------------------------------------------------------------------------------------

-- LESSON 6: Productionizing Across the Pipeline
-- Challenge Lab:
-- 1) Create a table called PL_GAME_LOGS (put it in the RAW schema).
-- It should have the same structure as the GAME_LOGS table. Same column(s) and column data type(s).
-- Solution below:
create or replace TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS 
    (
	RAW_LOG VARIANT
    );

-- Create Your New COPY INTO
-- 1) Write a COPY INTO statement that will load NOT just a specific named file into your table, but 
-- ANY file that lands in that folder. 
-- 2) Test your COPY INTO statement and when you see the results, make a note of how many files were loaded. 
-- Solution below:
copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
file_format = (format_name=ags_game_audience.raw.ff_json_logs);

-- Challenge Lab: Create a Step 2 Task to Run the COPY INTO
-- Create a Task that runs every 10 minutes. Name your task GET_NEW_FILES (put it in the RAW schema)
-- Copy and paste your COPY INTO into the body of your GET_NEW_FILES task. 
-- Run the EXECUTE TASK command a few times. New files are being added to the stage every 5 minutes, so keep that in mind as you test.  
-- Check to confirm that your task executed successfully and that the data from the files is being loaded as you expect.
-- Solution below:
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    warehouse = 'COMPUTE_WH'
    schedule = '10 minute'
as
copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
file_format = (format_name=ags_game_audience.raw.ff_json_logs);

-- Challenge Lab: Create a New JSON-Parsing View
-- 1) Using your LOGS view as a template, create a new view called PL_LOGS. The new view should pull from the new table.
-- 2) Check the new view to make sure all your rows appear in your new view as you expect them to.
-- Solution below:
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	RAW_LOG
) as 
    (
    SELECT
        RAW_LOG: ip_address:: text AS IP_ADDRESS,
        RAW_LOG: user_event:: text AS USER_EVENT,
        RAW_LOG: user_login:: text AS USER_LOGIN,
        RAW_LOG: datetime_iso8601:: TIMESTAMP_NTZ AS datetime_iso8601,
        RAW_LOG
    FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    WHERE IP_ADDRESS IS NOT NULL
    );

SELECT * FROM PL_LOGS;

-- Challenge Lab:  Modify the Step 4 MERGE Task
/*Files from a different stage are being loaded into a different raw table and
those rows are being parsed by a different JSON-Parsing view. The one thing we are not 
changing is our DESTINATION table. That table is still going to be LOGS_ENHANCED and 
the task that loads that table is still going to be your Merge Task, LOAD_LOGS_ENHANCED.  

The source stage is now UNI_KISHORE_PIPELINE instead of UNI_KISHORE.
The raw table being loaded is now PL_GAME_LOGS instead of GAME_LOGS. 
The original JSON-parsing view of LOGS has been replaced by PL_LOGS. 
The destination table has not changed. It should still be LOGS_ENHANCED.
Does any of the code need to be changed to make your merge use these new sources?
If so, change your merge code! 

When you've made the changes, manually run your MERGE task and make sure it works to 
INSERT all those new rows into your LOGS_ENHANCED table. This is the only object that does not change.
Our destination table remains the same, while all the source objects leading up to it have changed.*/
-- Solution below:
CREATE OR REPLACE task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse=COMPUTE_WH
    schedule='5 minute'
    AS
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING
    (
    SELECT
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        CONVERT_TIMEZONE( 'UTC', timezone, logs.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_Ltz) as DOW_NAME,
        TOD_NAME
        from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
        JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
    ) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name

WHEN NOT MATCHED THEN
INSERT 
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    )
VALUES
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    );

/*Truncate The Target Table

Before we begin testing our new pipeline, TRUNCATE the target table ENHANCED.LOGS_ENHANCED
so that we don't have the rows from our previous pipeline. Starting with zero rows gives us
an easier way to check that our new processes work the way we intend. */

TRUNCATE table ENHANCED.LOGS_ENHANCED;

/*The Current State of Things

Our process is looking good. We have:

Step 1 TASK (invisible to you, but running every 5 minutes)
Step 2 TASK that will load the new files into the raw table every 5 minutes (as soon as we turn it on).
Step 3 VIEW that is kind of boring but it does some light transformation (JSON-parsing) work for us.  
Step 4 TASK  that will load the new rows into the enhanced table every 5 minutes (as soon as we turn it on).*/

-- Turning on a task is done with a RESUME command:
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

-- Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

-- The data load is automated! All those steps have become a PIPELINE!

-- Checking Tallies Along the Way:
/*
STEP 1: Check the number of files in the stage, and multiply by 10. This is how many rows you should be expecting. 

STEP 2: The GET_NEW_FILES task grabs files from the UNI_KISHORE_PIPELINE stage and loads them into PL_GAME_LOGS.
How many rows are in PL_GAME_LOGS? 

STEP 3: The PL_LOGS view normalizes PL_GAME_LOGS without moving the data. Even though there are some filters in the
view, we don't expect to lose any rows. How many rows are in PL_LOGS?

STEP 4: The LOAD_LOGS_ENHANCED task uses the PL_LOGS view and 3 tables to enhance the data. We don't expect to lose
 any rows. How many rows are in LOGS_ENHANCED?
*/
--Step 1 - how many files in the bucket:
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Grant Serverless Task Management to SYSADMIN:
use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

-- Switch back to sysadmin
use role sysadmin;

-- Challenge Lab: Replace the WAREHOUSE Property in Your Tasks:
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    -- warehouse = 'COMPUTE_WH' -- comment this line, so the USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE will work
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- add this line
    schedule = '5 minute'
as
copy into AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
file_format = (format_name=ags_game_audience.raw.ff_json_logs);

CREATE OR REPLACE task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    -- warehouse=COMPUTE_WH -- comment this line, so the USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE will work
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- add this line
    after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES -- Remove the SCHEDULE property and have LOAD_LOGS_ENHANCED run each time GET_NEW_FILES completes
    -- schedule='5 minute'
    AS
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING
    (
    SELECT
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        CONVERT_TIMEZONE( 'UTC', timezone, logs.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_Ltz) as DOW_NAME,
        TOD_NAME
        from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
        JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
    ) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name

WHEN NOT MATCHED THEN
INSERT 
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    )
VALUES
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    );

-- LESSON 7: Data Engineer Practice Improvement & Cloud Foundations

-- Select with Metadata and Pre-Load JSON Parsing:
SELECT 
    METADATA$FILENAME as log_file_name, --new metadata column
    METADATA$FILE_ROW_NUMBER as log_file_row_id, --new metadata column
    current_timestamp(0) as load_ltz, --new local time of load
    get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
    get($1,'user_event')::text as USER_EVENT,
    get($1,'user_login')::text as USER_LOGIN,
    get($1,'ip_address')::text as IP_ADDRESS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');

-- Create a New Target Table ED_PIPELINE_LOGS to Match the Select:
CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
    (
    log_file_name VARCHAR(16777216),
    log_file_row_id NUMBER(18,0),
    load_ltz TIMESTAMP_LTZ(0),
    DATETIME_ISO8601 TIMESTAMP_NTZ(9),
    USER_EVENT VARCHAR(16777216),
    USER_LOGIN VARCHAR(16777216),
    IP_ADDRESS VARCHAR(16777216)
    );

-- Truncate the table rows that were input during the CTAS:
truncate table ED_PIPELINE_LOGS;

-- Reload the table using your COPY INTO:
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name,
    METADATA$FILE_ROW_NUMBER as log_file_row_id,
    current_timestamp(0) as load_ltz,
    get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
    get($1,'user_event')::text as USER_EVENT,
    get($1,'user_login')::text as USER_LOGIN,
    get($1,'ip_address')::text as IP_ADDRESS,   
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    )
file_format = (format_name = ff_json_logs);

-- Event-Driven Pipelines vs Time-Driven Pipelines
/*
A Time-Driven Pipeline is not always the best solution because it
can waste money looking for data that isn't there, or allow a backlog
to build up if it hasn't been scheduled correctly to meet demand. 

The major alternative to Time-Driven Pipelines are Event-Driven Pipelines
and they are made possible by a Snowflake object called a Snowpipe. 

Our Event-Driven Pipeline will "sleep" until a certain event takes place,
then it will wake up and respond to the event. In our case, the "event" we
care about is a new file being written to our bucket. When our pipe "hears"
that a file has arrived, it will grab the file and move it into Snowflake. 
*/

-- LESSON 8: Snowpipe

-- Create a Snowpipe:
CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM 
    (
    SELECT 
        METADATA$FILENAME as log_file_name,
        METADATA$FILE_ROW_NUMBER as log_file_row_id,
        current_timestamp(0) as load_ltz,
        get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
        get($1,'user_event')::text as USER_EVENT,
        get($1,'user_login')::text as USER_LOGIN,
        get($1,'ip_address')::text as IP_ADDRESS    
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    )
file_format = (format_name = ff_json_logs);

-- Challenge lab: Update the LOAD_LOGS_ENHANCED Task
/*
Your old pipeline had two tasks. Your new pipeline uses one task and one Snowpipe.
The task that continues to be used will need to be edited for the new configuration. 

Begin by TRUNCATING your LOGS_ENHANCED table so we can check the CURRENT pipeline and
not get confused by our previous pipeline's results. (If you want to create a clone of 
it first, just in case, go ahead and do that. You could call it LOGS_ENHANCED_BACKUP.

Edit the LOAD_LOGS_ENHANCED Task so it loads from ED_PIPELINE_LOGS instead of PL_LOGS.
If the task is running, you'll need to suspend it. 

Now that there is no ROOT task, you can't use the ROOT task to trigger the LOAD_LOGS_ENHANCED.
You need to set it back to being a task scheduled every 5 minutes. 

Resume the task.  (Remember to SUSPEND THE TASK before you stop for the day and turn it back 
on when you resume next time)*/

-- Solution below:
TRUNCATE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

CREATE OR REPLACE task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    warehouse=COMPUTE_WH -- comment this line, so the USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE will work
    -- USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- add this line
    -- after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES -- Remove the SCHEDULE property and have LOAD_LOGS_ENHANCED run each time GET_NEW_FILES completes
    schedule='5 minute'
    AS
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING
    (
    SELECT
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        CONVERT_TIMEZONE( 'UTC', timezone, logs.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_Ltz) as DOW_NAME,
        TOD_NAME
        from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS logs
        JOIN IPINFO_GEOLOC.demo.location loc
        ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address)
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
    ) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name

WHEN NOT MATCHED THEN
INSERT 
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    )
VALUES
    (
    IP_ADDRESS,
    GAMER_NAME,
    GAME_EVENT_NAME,
    GAME_EVENT_UTC,
    CITY,
    REGION,
    COUNTRY,
    GAMER_LTZ_NAME,
    GAME_EVENT_LTZ,
    DOW_NAME,
    TOD_NAME
    );

-- Check if Snowpipe seems like it is stalled out:
ALTER PIPE ags_game_audience.raw.PIPE_GET_NEW_FILES REFRESH;

-- To check that if pipe is running:
select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));

-- STREAMs
/*
STREAM will not replace that last task and it will not make it event-driven,
but it will make the pipeline more efficient. It will do this by allowing us to
use a technique called "Change Data Capture" which is why the diagram is labeled with "CDC."
*/

-- Create a stream that will keep track of changes to the table:
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

-- Look at the stream was created:
show streams;

-- Check to see if any changes are pending (expect FALSE the first time run):
select system$stream_has_data('ed_cdc_stream');

-- Query the stream:
select * 
from ags_game_audience.raw.ed_cdc_stream; 

-- To pause or unpause the pipe:
alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;

-- Process the Rows from the Stream
-- Make a note of how many rows are in the stream:
select * 
from ags_game_audience.raw.ed_cdc_stream; 

-- Process the stream by using the rows in a merge:
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT
            cdc.ip_address,
            cdc.user_login as GAMER_NAME,
            cdc.user_event as GAME_EVENT_NAME,
            cdc.datetime_iso8601 as GAME_EVENT_UTC,
            city,
            region,
            country,
            timezone as GAMER_LTZ_NAME,
            CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz,
            DAYNAME(game_event_ltz) as DOW_NAME,
            TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
 
-- All the rows from the stream should disappear: 
select * 
from ags_game_audience.raw.ed_cdc_stream;

-- Some improvements
/*With the PIPE and STREAM in place, we just need a task at the end
that pulls new data from the STREAM, instead of from the RAW data table.
We can use the MERGE statement we just tested.*/

-- Create a CDC-Fueled, Time-Driven Task:
-- Create a new task that uses the MERGE:
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
-- Resume the task so it is running:
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

-- Change a task so it is partially time-driven but also STREAM dependent!
-- Add A Stream Dependency to the Task Schedule:
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
when system$stream_has_data('ed_cdc_stream') -- Add STREAM dependency logic to the TASK header and replace the task. 
	as
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
-- Resume the task so it is running:
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

-- LESSON 9: Curated Data
/*
In some other organizations, a Data Engineer will move data beyond the Enhanced level,
into a Curated state. When that happens, the Data Engineer is adding additional processing
that may help analysts and data scientists do their work faster, and/or more effectively.
*/

-- Rolling Up Login and Logout Events with ListAgg
/*
This is a quick and easy way to aggregate rows, but our goal with rolling up the rows is
to compare the times the users logs in and out of the system so we can get a metric on how
long they played the game. We will need a more sophisticated method to get this done. 
*/

-- The ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear:
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;

-- Windowed Data for Calculating Time in Game Per Player:
select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

-- Code for the Heatgrid:
-- A case statement added to bucket the session lengths:
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;
