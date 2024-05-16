-- LESSON 1 - Getting Started with Streamlit in Snowflake
-- Streamlit apps are schema-level objects in Snowflake. 
-- Therefore, they are located in a schema under a database.
-- They also rely on virtual warehouses to provide the compute resource.
-- We recommend starting with X-SMALL warehouses and upgrade when needed.

-- To help your team create Streamlit apps successfully, consider running the following script.
-- Please note that this is an example setup. 
-- You can modify the script to suit your needs.

-- If you want to create a new database for Streamlit Apps, run
CREATE OR REPLACE DATABASE STREAMLIT_APPS;
-- If you want to create a specific schema under the database, run
CREATE OR REPLACE SCHEMA PUBLIC;
-- Or, you can use the PUBLIC schema that was automatically created with the database.

-- If you want all roles to create Streamlit apps in the PUBLIC schema, run
GRANT USAGE ON DATABASE STREAMLIT_APPS TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA STREAMLIT_APPS.PUBLIC TO ROLE PUBLIC;
GRANT CREATE STREAMLIT ON SCHEMA STREAMLIT_APPS.PUBLIC TO ROLE PUBLIC;
GRANT CREATE STAGE ON SCHEMA STREAMLIT_APPS.PUBLIC TO ROLE PUBLIC;

-- Don't forget to grant USAGE on a warehouse.
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE PUBLIC;

-- If you only want certain roles to create Streamlit apps, 
-- or want to enable a different location to store the Streamlit apps,
-- change the database, schema, and role names in the above commands.


-- Create a table called FRUIT_OPTIONS in the PUBLIC schema of your SMOOTHIES database.
-- Make sure it owned by the SYSADMIN Role. 
-- The table should have two columns:
-- First column should be named FRUIT_ID and hold a number. 
-- Second column should be named FRUIT_NAME and hold text up to 25 characters long. 
create table FRUIT_OPTIONS (
    FRUIT_ID INT,
    FRUIT_NAME VARCHAR(25)
    )
    -- comment = '<comment>';

    ALTER TABLE PUBLIC.FRUIT_OPTIONS OWNER TO SYSADMIN;

-- Build a FILE FORMAT without: 1) header rows and 2) the column delimiter is a % sign:
CREATE FILE FORMAT SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM
    TYPE=CSV,
    SKIP_HEADER=2,
    FIELD_DELIMITER='%',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
;

-- Loading Files Using a COPY INTO Statement:
COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM '@"SMOOTHIES"."PUBLIC"."%FRUIT_OPTIONS"/__snowflake_temp_import_files__/'
FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = (FORMAT_NAME=SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM) -- FILE FORMAT that was just created above
ON_ERROR=ABORT_STATEMENT
PURGE = TRUE;

-- VALIDATION_MODE to check whether the file would load in case of try to load it directly from the new stage:
COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM @smoothies.public.my_internal_stage
FILES = ('fruits_available_for_smoothies.txt')
FILE_FORMAT = (FORMAT_NAME=SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM) -- FILE FORMAT that was just created above
ON_ERROR=ABORT_STATEMENT
VALIDATION_MODE = RETURN_ERRORS
PURGE = TRUE;

-- Query the Not-Yet-Loaded Data Using the File Format
-- We can query a file that is sitting in a STAGE, by using the $ sign and ordinal positions.
-- Our file only has two columns, so $3, $4, and $5 (as shown below) would be empty.
-- Using the "SELECT $1" syntax is a great way to check how your FILE FORMAT performs on a particular file you plan to load:

-- SELECT $1, $2, $3, $4, $5
-- FROM @<stage_name>.<file_name>
-- (FILE_FORMAT => <file_format_name>);

SELECT $1, $2
FROM @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt
(FILE_FORMAT => MOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM);

-- Reorder Columns During the COPY INTO LOAD:
COPY INTO "SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"
FROM (select $2 as FRUIT_ID, $1 as FRUIT_NAME
from @smoothies.public.my_internal_stage/fruits_available_for_smoothies.txt)
FILE_FORMAT = (FORMAT_NAME=SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM)
ON_ERROR=ABORT_STATEMENT
PURGE= TRUE;

-- LESSON 2 - Moving Data from SiS to Snowflake
-- Create a Place to Store Order Data table in SMOOTHIES database:
CREATE TABLE SMOOTHIES.PUBLIC.ORDERS
(
ingredients varchar(200)
);

-- Make sure that a Snowflake Worksheet works:
insert into smoothies.public.orders(ingredients)
values ('Cantaloupe Guava Jackfruit Elderberries Figs ');

select * from smoothies.public.orders;

truncate table smoothies.public.orders; -- clean the table after the test

-- LESSON 4 - Prototype
-- Adding a column to the orders table:
ALTER TABLE SMOOTHIES.PUBLIC.ORDERS ADD COLUMN name_on_order VARCHAR (100);

-- Adding a boolean column named ORDER_FILLED to the ORDERS table:
ALTER TABLE SMOOTHIES.PUBLIC.ORDERS ADD COLUMN ORDER_FILLED BOOLEAN DEFAULT FALSE;

-- Mark some rows as "filled" before trying to get the app working:
update SMOOTHIES.PUBLIC.ORDERS
    set order_filled = true
    where name_on_order is null;

-- LESSON 5 - Pending Orders App Improvements:
create sequence order_seq
    start = 1
    increment = 1
    comment = 'Provide a unique id for smoothie orders';

-- Truncate the Orders table to remove all rows before adding a column with a UNIQUE ID:
truncate table smoothies.public.orders;

-- Add the Unique ID Column:
alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column

-- Adding a timestamp to sort the orders. That way, the kitchen staff will always be working on the oldest order first = FIFO.
-- Put the columns in a more intuitive order: in most tables, the unique id column is the first column, datetime stamps often come last:
create or replace table smoothies.public.orders (
       order_uid number(38,0) default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       order_ts timestamp_ltz(9) default current_timestamp(),
       constraint order_uid unique (order_uid)
);

-- LESSON 7 - Into to Variables and Variavle-Driven Loading
-- Create & Set a Local SQL Variable:
set mystery_bag = 'What is in here?';

select $mystery_bag;

-- Do More With More Variables:
set var1 = 2;
set var2 = 5;
set var3 = 7;

select $var1+$var2+$var3;

-- Simple User Defined Function (UDF), dollar sign symbol is needed when referring to a local variable:
create function sum_mystery_bag_vars(var1 number, var2 number, vars number)
    returns number as 'select var1+var2+var3';

select sum_mystery_bag_vars(12,36,204);

--  Using a System Function to Fix a Variable Value:
set alternating_caps_phrase = 'aLtErNaTiNg CaPs!':
select $alternating_caps_phrase;
-- Output: aLtErNaTiNg CaPs!

set alternating_caps_phrase = 'wHy ArE yOu LIkE tHiS?';
select initcap($alternating_caps_phrase);
-- Output: Why Are You Like This?

-- CHALLENGE LAB:  Write a UDF that Neutralizes Alternating Caps Phrases!
-- Your function should be in the UTIL_DB.PUBLIC schema. 
-- Your function should be named NEUTRALIZE_WHINING
-- Your function should accept a single variable of type TEXT. It won't matter what you name the variable.
-- Your function should return a TEXT value.

-- Solution: 
CREATE OR REPLACE FUNCTION UTIL_DB.PUBLIC.NEUTRALIZE_WHINING(input_text TEXT)
    RETURNS TEXT as 'select initcap(input_text)';

-- LESSON 10 - Using API data With Variables
select * from ORDERS;
alter table fruit_options add column search_on;

-- Copy name to seed the columns, then edit the problem rows:
update fruit_options
set search_on = 'Apple';--

select * from fruit_options;

update fruit_options
set search_on = 'Apple'
where fruit_name = 'Apples';