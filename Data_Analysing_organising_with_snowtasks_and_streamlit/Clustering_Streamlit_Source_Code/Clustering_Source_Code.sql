create database Healthcare_db;
create schema staging;
use database healthcare_db;
use schema staging;

CREATE OR REPLACE table covid_analysis(
Country Varchar(60),
Confirmed Int,
Death Int,
Recovered Int,
Active Int,
"New case" Int,
"New deaths" Int,
"New recovered" Int,
"Deaths / 100 Cases" float, 
"Recovered / 100 Cases" float,
"Deaths / 100 Recovered" float,
"Confirmed last week" Int,
"1 week change" Int,
 "1 week % increase" Int,
 "WHO Region" Varchar(100)
);
CREATE OR REPLACE TABLE covid_analysis_stage LIKE covid_analysis;

CREATE OR REPLACE STREAM covid_stage_stream
ON TABLE covid_analysis_stage;
INSERT INTO covid_analysis_stage (
  Country, Confirmed, Death, Recovered, Active, "New case", "New deaths", "New recovered",
  "Deaths / 100 Cases", "Recovered / 100 Cases", "Deaths / 100 Recovered",
  "Confirmed last week", "1 week change", "1 week % increase", "WHO Region"
)
VALUES (
  'India', 100000, 2000, 95000, 3000, 500, 10, 400, 2.0, 95.0, 2.1, 98000, 2000, 2, 'SEARO'
);

SELECT * FROM covid_stage_stream;


CREATE OR REPLACE TASK load_covid_task
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 15 14 * * * UTC'  -- 7:30 PM IST daily
AS
INSERT INTO covid_analysis (
  Country, Confirmed, Death, Recovered, Active, "New case", "New deaths", "New recovered",
  "Deaths / 100 Cases", "Recovered / 100 Cases", "Deaths / 100 Recovered",
  "Confirmed last week", "1 week change", "1 week % increase", "WHO Region"
)
SELECT 
  Country, Confirmed, Death, Recovered, Active, "New case", "New deaths", "New recovered",
  "Deaths / 100 Cases", "Recovered / 100 Cases", "Deaths / 100 Recovered",
  "Confirmed last week", "1 week change", "1 week % increase", "WHO Region"
FROM covid_stage_stream;

ALTER TASK load_covid_task RESUME;

SELECT NAME,
       STATE,
       SCHEDULED_TIME,
       COMPLETED_TIME,
       RETURN_VALUE,
       ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'LOAD_COVID_TASK'
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;

SELECT *
FROM INFORMATION_SCHEMA.OBJECT_DEPENDENCIES
WHERE OBJECT_NAME IN ('COVID_ANALYSIS', 'COVID_ANALYSIS_STAGE', 'COVID_STAGE_STREAM');

