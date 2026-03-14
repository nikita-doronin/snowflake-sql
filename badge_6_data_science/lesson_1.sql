-- LESSON 1: Set UP

-- Prepare the environment after account creation:
alter user <my_user_name> set default_role = 'SYSADMIN';
alter user <my_user_name> set default_warehouse = 'COMPUTE_WH';

-- Set default database and schema:
alter user <my_user_name> set default_namespace = 'UTIL_DB.PUBLIC';

-- Create sandbox DB:
use role SYSADMIN;
create database if not exists UTIL_DB;

-- Set up DORA:
use role accountadmin;
create or replace api integration dora_api_integration api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole' enabled = true api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

create or replace external function util_db.public.grader(        
 step varchar     
 , passed boolean     
 , actual integer     
 , expected integer    
 , description varchar) 
 returns variant 
 api_integration = dora_api_integration 
 context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
 as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'  
;  

-- Check  DORA configuration:
select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 