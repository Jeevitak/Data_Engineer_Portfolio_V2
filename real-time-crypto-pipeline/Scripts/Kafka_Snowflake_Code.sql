create database crypto_db;
use database crypto_db;
create schema staging;
use schema staging;

CREATE OR REPLACE TABLE crypto_prices (
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bitcoin_usd FLOAT,
    ethereum_usd FLOAT
);
alter table crypto_prices
add column dag_run_id string;
alter table crypto_prices
add column SOURCE string;

select *from crypto_prices
where dag_run_id is not null
and source is not null
order by timestamp desc;