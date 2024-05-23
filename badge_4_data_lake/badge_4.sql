-- LESSON 2 - Data Structuring & Stage Types

-- Structured data:
-- Structured data in a file (like a .txt or .csv) is arranged as a series of rows (often each line in the file is a row). 
-- Each row in the file is separated into columns. 

-- Semi-Structured data:
-- Semi-Structured data (in a file like .json, or .xml)  is data that has unpredictable levels of nesting and often each "row" of data can vary widely in the information it contains.
-- Semi-structured data stored in a flat file may have markup using angle brackets (like XML) or curly brackets (like JSON).
-- JSON, XML, Avro, Parquet, and ORC

-- Unstructured Data:
-- There is a third type of data file called Unstructured data. (File names will have extensions like .mp3, .mp4, .png, etc.)
-- Snowflake added support for Unstructured Data in August of 2021. 

-- Proof of Concept (POC):
-- Rapid Prototyping techniques for her Proof of Concept.
-- For Example: File Formats, Directory Tables, External Tables and Materialized Views

-- List the files in the Stages:
LIST @UNI_KLAUS_CLOTHING;
LIST @UNI_KLAUS_SNEAKERS;
LIST @UNI_KLAUS_ZMD;

-- LESSON 3 - Leavig the Data where it lands

-- Query Data in the ZMD:
select $1
from @uni_klaus_zmd;

-- Create a file format to test whether the carets are supposed to separate one row from another:
create file format zmd_file_format_1
RECORD_DELIMITER = '^';

-- Using the Exploratory File Format in a Query:
select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

-- Create a second exploratory file format:
-- Carets aren't the row separators, but they are the column separators, instead:
create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

-- Write a new File Format zmd_file_format_3:
create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'; 

select $1, $2
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

-- Challenge lab task: Rewrite zmd_file_format_1 to parse sweatsuit_sizes.txt:
create or replace file format zmd_file_format_1
FIELD_DELIMITER = '^'
RECORD_DELIMITER = ';';

select $1 as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 );

-- Challenge lab task: Rewrite zmd_file_format_2 to parse swt_product_line.txt:
create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE; -- delete spaces before values

select $1, $2, $3
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

/*Dealing with Unexpected Characters

Many data files use CRLF (Carriage Return Line Feed) as the record delimiter, 
so if a different record delimiter is used, the CRLF can end up displayed or loaded! 

When strange characters appear in your data, you can refine your select statement to deal with them. 
In SQL we can use ASCII references to deal with these characters. 

13 is the ASCII for Carriage return
10 is the ASCII for Line Feed

SQL has a function, CHR() that will allow you to reference ASCII characters by their numbers.
So, chr(13) is the same as the Carriage Return character and chr(10) is the same as the Line Feed character. 
In Snowflake, we can CONCATENATE two values by putting || between them (a double pipe). So we can look for 
CRLF by telling Snowflake to look for:

chr(13)||chr(10)*/

-- Example below:
select REPLACE($1,chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_2)
where sizes_available <> ''; -- filter out empty values

-- Convert Your Select to a View:
create or replace view zenas_athleisure_db.products.sweatsuit_sizes as 
select REPLACE($1,chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_2)
where sizes_available <> '';

select *
from zenas_athleisure_db.products.sweatsuit_sizes;

-- Challenge lab task: Make the Sweatband Product Line File Look Great:

-- REPLACE file format 2 so that the DELIMITERS are correct to process the sweatband data file. 
-- Remove leading spaces in the data with the TRIM_SPACE property. 
-- Remove CRLFs from the data (via your select statement).
-- If there are any weird, empty rows, remove them (also via the select statement).
-- Put a view on top of it to make it easy to query in the future! Name your view:  zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE
-- Don't forget to NAME the columns in your Create View statement. You can see the names you should use for your columns in the screenshot. 

-- Solution below:
create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

create or replace view zenas_athleisure_db.products.sweatband_product_Line as 
select 
    REPLACE($1,chr(13)||chr(10)) as PRODUCT_CODE,
    REPLACE($2,chr(13)||chr(10)) as HEADBAND_DESCRIPTION,
    REPLACE($3,chr(13)||chr(10)) as WRISTBAND_DESCRIPTION
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

select * from zenas_athleisure_db.products.sweatband_product_Line;

-- File format 3 is already working for the product coordination data set, since it doesn't have a lot going on.

-- Remove CRLFs from the data (via your select statement).
-- If there are any weird, empty rows, remove them (also via the select statement).
-- Put a view on top of it to make it easy to query in the future! Name your view:  zenas_athleisure_db.products.SWEATBAND_COORDINATION

-- Solution below:
create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACE = TRUE;

create or replace view zenas_athleisure_db.products.SWEATBAND_COORDINATION as
select
    $1 as PRODUCT_CODE,
    $2 as HAS_MATCHING_SWEATSUIT
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

select * from zenas_athleisure_db.products.SWEATBAND_COORDINATION;

-- LESSON 4 - Working with External Unstructured Data

-- Query with 2 Built-In Meta-Data Columns:
select metadata$filename, metadata$file_row_number
from @uni_klaus_clothing/90s_tracksuit.png;

-- Create a query that would GROUP BY the file name and 
-- use either the MAX function or a COUNT to get an idea of 
-- the comparative file size for all the files in the stage:
select metadata$filename, COUNT(metadata$file_row_number)
from @uni_klaus_clothing
group by metadata$filename;

-- Enabling, Refreshing and Querying Directory Tables:
-- Directory Tables
select * from directory(@uni_klaus_clothing);

-- Turn them on first:
alter stage uni_klaus_clothing 
set directory = (enable = true);

select * from directory(@uni_klaus_clothing);

-- Refresh the directory table:
alter stage uni_klaus_clothing refresh;

select * from directory(@uni_klaus_clothing);

-- Checking Whether Functions will Work on Directory Tables:
-- Testing UPPER and REPLACE functions on directory table:
select
    UPPER(RELATIVE_PATH) as uppercase_filename,
    REPLACE(uppercase_filename,'/') as no_slash_filename,
    REPLACE(no_slash_filename,'_',' ') as no_underscores_filename,
    REPLACE(no_underscores_filename,'.PNG') as just_words_filename
from directory(@uni_klaus_clothing);

-- Challenge lab task: Nest 4 Functions into 1 Statement:
-- Nest them all into a single column and name it "PRODUCT_NAME"

-- Solution below:
select REPLACE(
            REPLACE(
                REPLACE(
                    UPPER(RELATIVE_PATH), 
                '/', ''), 
            '_', ' '), 
        '.PNG', '') as PRODUCT_NAME
from directory(@uni_klaus_clothing);

-- Create an Internal Table in the Zena Database:
-- Create an internal table for some sweat suit info:
create or replace TABLE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS (
	COLOR_OR_STYLE VARCHAR(25),
	DIRECT_URL VARCHAR(200),
	PRICE NUMBER(5,2)
);

-- Fill the new table with some data
insert into  ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS 
          (COLOR_OR_STYLE, DIRECT_URL, PRICE)
values
('90s', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/90s_tracksuit.png',500)
,('Burgundy', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/burgundy_sweatsuit.png',65)
,('Charcoal Grey', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/charcoal_grey_sweatsuit.png',65)
,('Forest Green', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/forest_green_sweatsuit.png',65)
,('Navy Blue', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/navy_blue_sweatsuit.png',65)
,('Orange', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/orange_sweatsuit.png',65)
,('Pink', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/pink_sweatsuit.png',65)
,('Purple', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/purple_sweatsuit.png',65)
,('Red', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/red_sweatsuit.png',65)
,('Royal Blue',	'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/royal_blue_sweatsuit.png',65)
,('Yellow', 'https://uni-klaus.s3.us-west-2.amazonaws.com/clothing/yellow_sweatsuit.png',65);

-- Challenge lab task: Join the directory table and the new sweatsuits table:
-- Solution below:
select
    color_or_style,
    direct_url,
    price,
    size as image_size,
    last_modified as image_last_modified
from sweatsuits s
join directory(@uni_klaus_clothing) d
on split_part(DIRECT_URL,'/',-1)=split_part(relative_path,'/',-1);

-- 2nd way jon:
select
    color_or_style,
    direct_url,
    price,
    size as image_size,
    last_modified as image_last_modified
from sweatsuits s
join directory(@uni_klaus_clothing) d
on d.relative_path = SUBSTR(s.direct_url,54,50) :

-- 3rd way join - internal table, directory table, and view based on external data:
create or replace view ZENAS_ATHLEISURE_DB.PRODUCTS.CATALOG as
select color_or_style
, direct_url
, price
, size as image_size
, last_modified as image_last_modified
, sizes_available
from sweatsuits 
join directory(@uni_klaus_clothing) 
on relative_path = SUBSTR(direct_url,54,50)
cross join sweatsuit_sizes;

-- Add the Upsell Table and Populate It
-- Add a table to map the sweat suits to the sweat band sets:
create table ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE varchar(25)
,UPSELL_PRODUCT_CODE varchar(10)
);

--populate the upsell table
insert into ZENAS_ATHLEISURE_DB.PRODUCTS.UPSELL_MAPPING
(
SWEATSUIT_COLOR_OR_STYLE
,UPSELL_PRODUCT_CODE 
)
VALUES
('Charcoal Grey','SWT_GRY')
,('Forest Green','SWT_FGN')
,('Orange','SWT_ORG')
,('Pink', 'SWT_PNK')
,('Red','SWT_RED')
,('Yellow', 'SWT_YLW');

-- View for the Athleisure Web Catalog Prototype:
-- A single view she can query for her website prototype
create view catalog_for_website as 
select color_or_style
,price
,direct_url
,size_list
,coalesce('BONUS: ' ||  headband_description || ' & ' || wristband_description, 'Consider White, Black or Grey Sweat Accessories')  as upsell_product_desc
from
(   select color_or_style, price, direct_url, image_last_modified,image_size
    ,listagg(sizes_available, ' | ') within group (order by sizes_available) as size_list
    from catalog
    group by color_or_style, price, direct_url, image_last_modified, image_size
) c
left join upsell_mapping u
on u.sweatsuit_color_or_style = c.color_or_style
left join sweatband_coordination sc
on sc.product_code = u.upsell_product_code
left join sweatband_product_line spl
on spl.product_code = sc.product_code
where price < 200 -- high priced items like vintage sweatsuits aren't a good fit for this website
and image_size < 1000000 -- large images need to be processed to a smaller size
;

-- LESSON 6 - GeoSpatial Views:
select $1 from @TRAILS_GEOJSON
(file_format => FF_JSON);

select $1 from @TRAILS_PARQUET
(file_format => FF_PARQUET);

-- Longitudes are between 0 (the prime meridian) and 180. So no more than 3 digits are needed to the left of the decimal for longitude data.
-- If we cast both longitude and latitude data as NUMBER(11,8) we should be safe.  We have included the code for this select statement below. 
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL as
select 
    $1:sequence_1 as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng,
    $1:longitude::number(11,8) as lat
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

-- Use || to Chain Lat and Lng Together into Coordinate Sets
-- Using concatenate to prepare the data for plotting on a map:
select top 100 
    lng||' '||lat as coord_pair,
    'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

-- Add a column coord_pair:
create or replace view cherry_creek_trail as
select 
    $1:sequence_1 as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng,
    $1:longitude::number(11,8) as lat,
    lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

-- Snowflakes LISTAGG function and the new COORD_PAIR column to make LINESTRINGS could be paste into WKT Playground:
select 
'LINESTRING('||
listagg(coord_pair, ',') 
within group (order by point_id)
||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

-- Normalizing the Data Without Loading It, Visually Display the geoJSON Data:
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS as
select
    $1:features[0]:properties:Name::string as feature_name,
    $1:features[0]:geometry:coordinates::string as feature_coordinates,
    $1:features[0]:geometry::string as geometry,
    $1:features[0]:properties::string as feature_properties,
    $1:crs:properties:name::string as specs,
$1 as whole_object
from @trails_geojson (file_format => ff_json);

-- LESSON 7 - Exploring GeoSpatial Functions

-- Create a view that will have similar columns to DENVER_AREA_TRAILS 
-- Even though this data started out as Parquet, and we're joining it with geoJSON data
-- So let's make it look like geoJSON instead.

-- Create a View on Cherry Creek Data to Mimic the Other Trail Data:
create or replace view DENVER_AREA_TRAILS_2 as
select 
trail_name as feature_name
,'{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry
,st_length(to_geography(geometry)) as trail_length
from cherry_creek_trail
group by trail_name;

-- Use A Union All to Bring the Rows Into a Single Result Set:
-- Create a view that will have similar columns to DENVER_AREA_TRAILS:
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length
from DENVER_AREA_TRAILS_2;

-- Converting each list of coordinates into a GeoSpatial Object type called a LINESTRING:
select 
    feature_name,
    to_geography(geometry) as my_linestring,
    trail_Length
from DENVER_AREA_TRAILS union all
select
    feature_name,
    to_geography(geometry) as my_linestring,
    trail_length
from DENVER_AREA_TRAILS_2;

-- Add more GeoSpatial Calculations to get more GeoSpecial Information:
create view TRAILS_AND_BOUNDARIES as
select feature_name
    to_geography(geometry) as my_linestring,
    st_xmin(my_linestring) as min_eastwest,
    st_xmax(my_linestring) as max_eastwest,
    st_ymin(my_linestring) as min_northsouth,
    st_ymax(my_linestring) as max_northsouth,
    trail_length
from DENVER_AREA_TRAILS
union all
select feature_name
    to_geography(geometry) as my_linestring,
    st_xmin(my_linestring) as min_eastwest,
    st_xmax(my_linestring) as max_eastwest,
    st_ymin(my_linestring) as min_northsouth,
    st_ymax(my_linestring) as max_northsouth,
    trail_length
from DENVER_AREA_TRAILS_2;

select * from TRAILS_AND_BOUNDARIES;

-- A Polygon Can be Used to Create a Bounding Box:
select
    min (min_eastwest) as western_edge,
    min (min_northsouth) as southern_edge,
    max(max_eastwest) as eastern_edge,
    max (max_northsouth) as northern_edge
from trails_and_boundaries;

select 'POLYGON(('||
    min(min_eastwest)||' '||max (max_northsouth)||','||
    max(max_eastwest)||' '||max (max_northsouth)||'.'||
    max(max_eastwest)||' '||min(min_northsouth)||','||
    min(min_eastwest)||' '||max (max_northsouth)||'))' as my_polygon
from trails_and_boundaries;

-- LESSON 8 - Supercharging Development with Marketplace Data
ALTER DATABASE OPENSTREETMAP_DENVER RENAME TO SONRA_DENVER_CO_USA_FREE;

-- Using Variables in Snowflake Worksheets:
-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lat='-104.97300245114094';
set mc_lng='39.76471253574085';

-- Confluence Park into a Variable (loc for location):
set loc_lat='-105.00840763333615'; 
set loc_lng='39.754141917497826';

-- Test your variables to see if they work with the Makepoint function:
select st_makepoint($mc_lat,$mc_lng) as melanies_cafe_point;
select st_makepoint($loc_lat,$loc_lng) as confluent_park_point;

-- Use the variables to calculate the distance from 
-- Melanie's Cafe to Confluent Park:
select st_distance(
        st_makepoint($mc_lat,$mc_lng)
        ,st_makepoint($loc_lat,$loc_lng)
        ) as mc_to_cp;

-- This version uses two sets of VARIABLES:
select
    st_distance
    (
        st_makepoint($mc_lat,$mc_lng),
        st_makepoint($loc_lat,$loc_lng)
    ) as mo_to_cp;

-- This version uses one set of CONSTANTS and one set of VARIABLES:
select
    st_distance
    (
        st_makepoint('-104.97300245114094','39.76471253574085'),
        st_makepoint ($loc_Lat,$loc_Lng)
    ) as mo_to_cp;


-- Pass in the point we want to measure the distance FROM.
-- "location" for "LOC", LOC_LAT as the Latitude and LOC_LNG as the Longitude.
-- Coordinates have a lot of digits behind the decimal, so we'll give them up to 32 decimal point spaces at number(38,32).

-- The number we return will be in METERS, and it will already have a Data Type of FLOAT.
-- So, unless we want to CAST it before we RETURN it, le?t's just accept that it comes back as Type FLOAT.
CREATE FUNCTION distance_to_mc(loc_lat number(38,32), loc_lng number(38,32))
  RETURNS FLOAT
  AS
  $$
    -- <function code will be here>
  $$
  ;

-- Create the UDF (User-Defined Function):
CREATE OR REPLACE FUNCTION distance_to_mc(loc_lat number(38,32), loc_lng number(38,32))
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,st_makepoint(loc_lat,loc_lng)
        )
  $$
  ;

-- Test the New Function:
-- Tivoli Center into the variables:
set tc_lat='-105.00532059763648'; 
set tc_lng='39.74548137398218';

select distance_to_mc($tc_lat,$tc_lng);

-- Create a List of Competing Juice Bars in the Area and convert the List into a View:
create or replace view COMPETITION as
select * 
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

-- Which Competitor is Closest to Melanie's:
SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

-- Changing the Function to Accept a GEOGRAPHY Argument:
CREATE OR REPLACE FUNCTION distance_to_mc(lat_and_lng GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
   st_distance(
        st_makepoint('-104.97300245114094','39.76471253574085')
        ,lat_and_lng
        )
  $$
  ;

SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_from_melanies
 ,*
FROM  competition
ORDER by distance_from_melanies;

-- When speaking about a FUNCTION plus its ARGUMENTS we can refer to it as the FUNCTION SIGNATURE:
-- Different Options, Same Outcome:
-- Tattered Cover Bookstore McGregor Square:
set tcb_lat='-104.9956203'; 
set tcb_lng='39.754874';

-- This will run the first version of the UDF:
select distance_to_mc($tcb_lat,$tcb_lng);

-- This will run the second version of the UDF, bc it converts the coords 
-- to a geography object before passing them into the function:
select distance_to_mc(st_makepoint($tcb_lat,$tcb_lng));

-- This will run the second version bc the Sonra Coordinates column
-- contains geography objects already:
select 
    name,
    distance_to_mc(coordinates) as distance_to_melanies,
    ST_ASWKT(coordinates)
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

-- Challenge lab task: Create a View of Bike Shops in the Denver Data
-- You can find the shops in either the V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES 
-- or the V_OSM_DEN_SHOP table. The benefit of using the more specific view is that the 
-- columns included are more directly related to a bike shop. 
-- You can use a WHERE <column> = 'bicycle' -- you just have to figure out which column. 
-- Be sure to include a column called DISTANCE_TO_MELANIES that calculates the distance to Melanie's Café for each Bike Shop.

-- Solution below:
create or replace view DENVER_BIKE_SHOPS as
select
    name,
    distance_to_mc(coordinates) as distance_to_melanies,
    ST_ASWKT(coordinates) as coordinates_wkt
from SONRA_DENVER_CO_USA_FREE.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
where SHOP ilike 'bicycle';

-- LESSON 9

-- Materialized Views:
-- A Materialized View is like a view that is frozen in place (more or less looks and acts like a table).
-- The big difference is that if some part of the underlying data changes,  Snowflake recognizes the need to refresh it, automatically.
-- People often choose to create a materialized view if they have a view with intensive logic that they query often but that does NOT change often.
-- We can't use a Materialized view on any of our trails data because you can't put a materialized view directly on top of staged data. 

-- External Tables:
-- An External Table is a table put over the top of non-loaded data (sounds like our recent views, right?).
-- An External Table points at a stage folder(yep, we know how to do that!) and includes a reference to a file format (or formatting attributes)
-- much like what we've been doing with our views for most of this workshop! Seems very straightforward and something within reach-- given what 
-- we've already learned in this workshop!
-- But, if we look at docs.snowflake.com the syntax for External tables looks intimidating. Let's break it down into what we can easily understand 
-- and have experience with, and the parts that are little less straightforward. 

-- Extenral table creation:
select * from mels_smoothie_challenge_db.trails.cherry_creek_trail;

-- External Table so let's change the name of our view to have "V_" in front of the name. 
-- That way we can create a table that starts with "T_":
alter view mels_smoothie_challenge_db.trails.cherry_creek_trail
rename to mels_smoothie_challenge_db.trails.v_cherry_creek_trail;

create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(50) as (metadata$filename::varchar(50))
) 
location= @trails_parquet
auto_refresh = true
file_format = (type = parquet);

-- Run the GET_DDL() function to get a copy of our view code:
select get_ddl('view','mels_smoothie_challenge_db.trails.v_cherry_creek_trail');

-- Paste the code output from get_ddl function below:
create or replace view V_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR
) as
select 
 $1:sequence_1 as point_id,
 $1:trail_name::varchar as trail_name,
 $1:latitude::number(11,8) as lng,
 $1:longitude::number(11,8) as lat,
 lng||' '||lat as coord_pair
from @trails_parquet
(file_format => ff_parquet)
order by point_id;

-- The Full External Table query:
create or replace external table mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL
    (
	POINT_ID number as ($1:sequence_1::number),
	TRAIL_NAME varchar(100) as  ($1:trail_name::varchar),
	LNG number(11,8) as ($1:latitude::number(11,8)),
	LAT number(11,8) as ($1:longitude::number(11,8)),
	COORD_PAIR varchar(50) as (lng::varchar||' '||lat::varchar)
    ) 
location= @mels_smoothie_challenge_db.trails.trails_parquet
auto_refresh = true
file_format = mels_smoothie_challenge_db.trails.ff_parquet;

-- Challenge lab task: Create a Materialized View on Top of the External Table
create secure materialized view mels_smoothie_challenge_db.trails.smv_cherry_creek_trail as 
select * from mels_smoothie_challenge_db.trails.t_cherry_creek_trail;

-- Iceberg Tables:
-- Iceberg is an open-source table type, which means a private company does not own the technology.
-- Iceberg Table technology is not proprietary. 

-- Iceberg Tables are a layer of functionality you can lay on top of parquet files 
-- (just like the Cherry Creek Trails file we've been using) that will make files behave more like loaded data.
-- In this way, it's like a file format, but also MUCH more.

-- Iceberg Table data will be editable via Snowflake! Read that again.
-- Not just the tables are editable (like the table name), but the data they make available 
-- (like the data values in columns and rows). So, you will be able to create an Iceberg Table in Snowflake, 
-- on top of a set of parquet files that have NOT BEEN LOADED into Snowflake, and then run INSERT and UPDATE statements on the data using SQL