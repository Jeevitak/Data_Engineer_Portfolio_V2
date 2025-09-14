use role sysadmin;
create database if not exists sandbox;
use database sandbox;
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

use schema stage_sch;

create file format if not exists stage_sch.csv_file_format
type = 'csv'
compression = 'auto'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
field_optionally_enclosed_by = '\042'
null_if = ('\\N');

create stage stage_sch.csv_stg
directory = (enable=True)
comment = 'this is snowflake internal stage';

create or replace tag
common.pii_policy_tag
allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
comment = 'This is PII policy tag object';

create or replace masking policy
common.email_masking_policy as (email_text string)
returns string ->
to_varchar('**EMAIL**');

create or replace masking policy
common.phone_masking_policy as (phone string)
returns string ->
to_varchar('**phone**')

grant usage on warehouse ADHOC_WH to role sysadmin;
ls @stage_sch.csv_stg/initial;