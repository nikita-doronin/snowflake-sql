-- Basic examples:
select 'hello';

select 'hello' as greeting;

show databases;

show schemas;

-- Create a new table:
use role sysadmin;
create or replace table GARDEN_PLANTS.VEGGIES.ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   ); 


-- Insert a row of data:
USE WAREHOUSE COMPUTE_WH;
INSERT INTO ROOT_DEPTH (
	ROOT_DEPTH_ID ,
	ROOT_DEPTH_CODE ,
	ROOT_DEPTH_NAME ,
	UNIT_OF_MEASURE ,
	RANGE_MIN ,
	RANGE_MAX 
)

VALUES
(
    1,
    'S',
    'Shallow',
    'cm',
    30,
    45
)
;

--To add more than one row at a time:
insert into root_depth (root_depth_id, root_depth_code
                        , root_depth_name, unit_of_measure
                        , range_min, range_max)  
values
 (5,'X','short','in',66,77)
,(8,'Y','tall','cm',98,99)
;

-- To remove a row you do not want in the table
delete from root_depth
where root_depth_id = 9;

-- To change a value in a column for one particular row
update root_depth
set root_depth_id = 7
where root_depth_id = 9;

--T o remove all the rows and start over
truncate table root_depth;

-- Rename table
alter table old_table_name rename to new_table_name;

-- Rename database
ALTER DATABASE IF EXISTS old_name RENAME TO new_name;

-- Another example
create table garden_plants.veggies.vegetable_details
(
plant_name varchar(25)
, root_depth_code varchar(1)    
);

-- Empty out the table.
TRUNCATE TABLE GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS;

-- Creating file formats
create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '|' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;

create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

-- Delete the data from a table
select * from vegetable_details
where plant_name = 'Spinach'
and root_depth_code = 'D';

delete from vegetable_details
where plant_name = 'Spinach'
and root_depth_code = 'D';

-- LESSON 7
SELECT * 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA;

-- Checking for Schemas by Name:
SELECT * 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES'); 

-- Counting the Number of Correctly Named Schemas:
SELECT count(*) as SCHEMAS_FOUND, '3' as SCHEMAS_EXPECTED 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES'); 

-- Get a list of stage:
list @like_a_window_into_an_s3_bucket;

-- LESSON 8
create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

copy into vegetable_details_soil_type -- a table
from @util_db.public.like_a_window_into_an_s3_bucket -- a stage object
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt') -- a file
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW ); -- a file format

-- Create file format:
create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '|' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;

create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;
    
-- Query data before load it. Look at the data in a file we are about to load and see how the data changes based on what we tell Snowflake about how the data is formatted.
-- The data in the file, with no FILE FORMAT specified:
select $1
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv;

-- Same file but with one of the file formats we created earlier:
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

-- Same file but with the other file format we created earlier:
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

-- Create a new file format with name L8_CHALLENGE_FF:
create file format garden_plants.veggies.L8_CHALLENGE_FF
    TYPE = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    FIELD_DELIMITER = '|' --pipes as column separators
    SKIP_HEADER = 1 --one header row to skip
    ;

-- Create a new file format with name L8_CHALLENGE_FF and FIELD_DELIMITER == Tab(spacees):
CREATE OR REPLACE FILE FORMAT garden_plants.veggies.L8_CHALLENGE_FF
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = '\t'
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'
TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
ESCAPE = 'NONE' 
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'AUTO' 
TIMESTAMP_FORMAT = 'AUTO'
NULL_IF = ('\\N');

-- Using the file format thaw was just created:
select $1, $2, $3
from @util_db.public.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L8_CHALLENGE_FF);

-- Create a Soil Type Look Up Table:
create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
 );

--Create a COPY INTO Statement to Load the File into the Table:
copy into LU_SOIL_TYPE -- a table
from @util_db.public.like_a_window_into_an_s3_bucket -- a stage object
files = ( 'LU_SOIL_TYPE.tsv') -- a file
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.L8_CHALLENGE_FF ); -- a file format
select * from LU_SOIL_TYPE;

--Choosing a File Format, write the COPY INTO, Load the File into the Table:
create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT(
PLANT_NAME varchar(50),	
UOM varchar(1),
LOW_END_OF_RANGE number,
HIGH_END_OF_RANGE number
 );

copy into VEGETABLE_DETAILS_PLANT_HEIGHT -- a table
from @util_db.public.like_a_window_into_an_s3_bucket -- a stage object
files = ( 'veg_plant_height.csv') -- a file
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW ); -- a file format
select * from VEGETABLE_DETAILS_PLANT_HEIGHT;

--LESSON 9
--Create a New Database and Table:
use role sysadmin;

-- Create a new database and set the context to use the new database:
CREATE DATABASE LIBRARY_CARD_CATALOG COMMENT = 'DWW Lesson 9 ';
USE DATABASE LIBRARY_CARD_CATALOG;

-- Create Author table:
CREATE OR REPLACE TABLE AUTHOR (
   AUTHOR_UID NUMBER 
  ,FIRST_NAME VARCHAR(50)
  ,MIDDLE_NAME VARCHAR(50)
  ,LAST_NAME VARCHAR(50)
);

-- Insert the first two authors into the Author table:
INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
(1, 'Fiona', '','Macdonald')
,(2, 'Gian','Paulo','Faleschini');

-- Look at your table with it's new rows:
SELECT * 
FROM AUTHOR;

-- Create a Sequence
-- A sequence is a counter. It can help to create unique ids for table rows.
-- There are ways to create unique ids within a single table called an AUTO-INCREMENT column.
-- Those are easy to set up and work well in a single table. A sequence can give you the power
-- to split information across different tables and put the same ID in all tables as a way to make it easy to link them back together later. 

create sequence SEQ_AUTHOR_UID -- create or replace sequence SEQ_AUTHOR_UID
start = 1
increment = 1
comment = 'Use this to fill in AUTHOR_UID';


use role sysadmin;
--See how the nextval function works:
SELECT SEQ_AUTHOR_UID.nextval;

-- Reset the Sequence then Add Rows to Author:
use role sysadmin;

-- Drop and recreate the counter (sequence) so that it starts at 3
-- Then we'll add the other author records to our author table:
CREATE OR REPLACE SEQUENCE "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_AUTHOR_UID" 
START 3 
INCREMENT 1 
COMMENT = 'Use this to fill in the AUTHOR_UID every time you add a row';

-- Add the remaining author records and use the nextval function instead 
-- of putting in the numbers
INSERT INTO AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
(SEQ_AUTHOR_UID.nextval, 'Laura', 'K','Egendorf')
,(SEQ_AUTHOR_UID.nextval, 'Jan', '','Grover')
,(SEQ_AUTHOR_UID.nextval, 'Jennifer', '','Clapp')
,(SEQ_AUTHOR_UID.nextval, 'Kathleen', '','Petelinsek');


-- Create a 2nd Counter, a Book Table, and a Mapping Table:
USE DATABASE LIBRARY_CARD_CATALOG;

-- Create a new sequence, this one will be a counter for the book table:
CREATE OR REPLACE SEQUENCE "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_BOOK_UID" 
START 1 
INCREMENT 1 
COMMENT = 'Use this to fill in the BOOK_UID everytime you add a row';

-- Create the book table and use the NEXTVAL as the
-- default value each time a row is added to the table:
CREATE OR REPLACE TABLE BOOK
( BOOK_UID NUMBER DEFAULT SEQ_BOOK_UID.nextval
 ,TITLE VARCHAR(50)
 ,YEAR_PUBLISHED NUMBER(4,0)
);

-- Insert records into the book table
-- You don't have to list anything for the
-- BOOK_UID field because the default setting
-- will take care of it for you:
INSERT INTO BOOK(TITLE,YEAR_PUBLISHED)
VALUES
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

-- Create the relationships table,
-- this is sometimes called a "Many-to-Many table":
CREATE TABLE BOOK_TO_AUTHOR
(  BOOK_UID NUMBER
  ,AUTHOR_UID NUMBER
);

-- Insert rows of the known relationships:
INSERT INTO BOOK_TO_AUTHOR(BOOK_UID,AUTHOR_UID)
VALUES
 (1,1)  -- This row links the 2001 book to Fiona Macdonald
,(1,2)  -- This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  -- Links 2006 book to Laura K Egendorf
,(3,4)  -- Links 2008 book to Jan Grover
,(4,5)  -- Links 2016 book to Jennifer Clapp
,(5,6); -- Links 2015 book to Kathleen Petelinsek

-- Check your work by joining the 3 tables together,
-- should get 1 row for every author:
select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 

-- LESSON 10 - Intro to Semi-Structured Data
-- Create a Table Raw JSON Data
-- JSON DDL Scripts:
USE LIBRARY_CARD_CATALOG;

-- Create an Ingestion Table for JSON Data:
CREATE TABLE LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON 
(
  RAW_AUTHOR VARIANT
);

-- Create a File Format to Load the JSON Data:
CREATE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE;

-- Load data:
copy into author_ingest_json -- a table
from @util_db.public.like_a_window_into_an_s3_bucket -- a stage object
files = ( 'author_with_header.json') -- a file
file_format = ( format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT ); -- a file format
-- retruns entire record
select raw_author
from author_ingest_json;

-- Query the JSON Data,
-- returns AUTHOR_UID value from top-level object's attribute:
select raw_author:AUTHOR_UID
from author_ingest_json;

-- Returns the data in a way that makes it look like a normalized table:
SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;


-- LESSON 11 - Nested Semi-Structured Data
-- Create a Table & File Format for Nested JSON Data
-- Create an Ingestion Table for the NESTED JSON Data:
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON 
(
  "RAW_NESTED_BOOK" VARIANT
);

-- Load data:
copy into NESTED_INGEST_JSON -- a table
from @util_db.public.like_a_window_into_an_s3_bucket -- a stage object
files = ( 'json_book_author_nested.json') -- a file
file_format = ( format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT ); -- a file format
-- retruns entire record
select raw_author
from author_ingest_json;

-- truncate table NESTED_INGEST_JSON;

-- Query the Nested JSON Data:
-- A few simple queries:
SELECT RAW_NESTED_BOOK
FROM NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:year_published
FROM NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:authors
FROM NESTED_INGEST_JSON;

-- Use these example flatten commands to explore flattening the nested book and author data:
SELECT value:first_name
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

SELECT value:first_name
FROM NESTED_INGEST_JSON
,table(flatten(RAW_NESTED_BOOK:authors));

-- Add a CAST command to the fields returned:
SELECT value:first_name::VARCHAR, value:last_name::VARCHAR
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

-- Assign new column  names to the columns using "AS":
SELECT value:first_name::VARCHAR AS FIRST_NM
, value:last_name::VARCHAR AS LAST_NM
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);


-- Create a Database, Table & File Format for Nested JSON Data
-- Create a new database to hold the Twitter file:
CREATE DATABASE SOCIAL_MEDIA_FLOODGATES 
COMMENT = 'There is so much data from social media - flood warning';

USE DATABASE SOCIAL_MEDIA_FLOODGATES;

-- Create a table in the new database:
CREATE TABLE SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST 
("RAW_STATUS" VARIANT) 
COMMENT = 'Bring in tweets, one row per tweet or status entity';

-- Create a JSON file format in the new database:
CREATE FILE FORMAT SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE 
STRIP_OUTER_ARRAY = TRUE 
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE;

-- Load a json data:
copy into TWEET_INGEST -- a table
from @util_db.public.like_a_window_into_an_s3_bucket -- a stage object
files = ( 'nutrition_tweets.json') -- a file
file_format = ( format_name=SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT  ); -- a file format
-- retruns entire record
select *
from TWEET_INGEST;

-- Query the Nested JSON Tweet Data,
-- select statements as seen in the video:
SELECT RAW_STATUS
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities:hashtags
FROM TWEET_INGEST;

-- Explore looking at specific hashtags by adding bracketed numbers
-- This query returns just the first hashtag in each tweet:
SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST;

-- This version adds a WHERE clause to get rid of any tweet that 
-- doesn't include any hashtags:
SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST
WHERE RAW_STATUS:entities:hashtags[0].text is not null;

-- Perform a simple CAST on the created_at key
-- Add an ORDER BY clause to sort by the tweet's creation date:
SELECT RAW_STATUS:created_at::DATE
FROM TWEET_INGEST
ORDER BY RAW_STATUS:created_at::DATE;

-- Flatten statements that return the whole hashtag entity:
SELECT value
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

SELECT value
FROM TWEET_INGEST
,TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

-- Flatten statement that restricts the value to just the TEXT of the hashtag:
SELECT value:text
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

-- Flatten and return just the hashtag text, CAST the text as VARCHAR:
SELECT value:text::VARCHAR
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

-- Flatten and return just the hashtag text, CAST the text as VARCHAR
-- Use the AS command to name the column:
SELECT value:text::VARCHAR AS THE_HASHTAG
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

-- Add the Tweet ID and User ID to the returned table:
SELECT RAW_STATUS:user:id AS USER_ID
,RAW_STATUS:id AS TWEET_ID
,value:text::VARCHAR AS HASHTAG_TEXT
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

-- Create a View of the Tweet Data Looking "Normalized":
create or replace view SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED as
(SELECT RAW_STATUS:user:id AS USER_ID
,RAW_STATUS:id AS TWEET_ID
,value:text::VARCHAR AS HASHTAG_TEXT
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags)
);
