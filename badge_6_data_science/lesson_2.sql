-- LESSON 2: Snowflake's Cortex Playground

/*
Usage Quotas:
- A high utilization period might mean my access is throttled.
- If my access is throttled I might see an error response.
- Because of a trial account, it could only be able to use up to 10 credits per 24 hour period.

Cost of Using LLMs:
There are several different LLM functions available in Snowflake.
The playground prompts we just ran were carried out using the COMPLETE function.

For example in Badge 2: CMCW we learned that when using an eXtra-small Snowflake Warehouse one credit is roughly one hour of time. Credit usage for LLM models is very different.

For LLM models, credits are consumed based on how long and complex your question is and also how long and complex the answer is. In fact, each word (or sometimes phrase) is counted as a TOKEN.
So an 8-word question with a 200-word response could clock in at 208 tokens used. For simplicity, we'll presume that each word equals one token.
*/

-------------------------------------------------------------------------------
-- Check for Lesson completion.
USE ROLE SYSADMIN;
USE SCHEMA UTIL_DB.PUBLIC;

-- DO NOT EDIT ANYTHING BELOW THIS LINE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DSCW01' as step 
   ,( select  iff(count(*)>=5, 5, 0)
     from (
       select model_name
       from SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AISQL_USAGE_HISTORY
       where function_name ilike '%AI COMPLETE%'
       group by model_name
          )
     ) as actual 
   , 5 as expected 
   ,'Used Different models when exploring Cortex Playground' as description
); 