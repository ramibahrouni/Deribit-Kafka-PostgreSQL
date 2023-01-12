#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  BEGIN;
  create table if not exists deribit_prices (
    instrument_name varchar(155),
    underlying_price float,
    timestamp bigint,
    settlement_price float,
    open_interest float,
    min_price float,
    max_price float,
    mark_price float,
    mark_iv float,
    last_price float,
    interest_rate float,
    index_price float,
    bid_iv float,
    best_bid_price float,
    best_bid_amount float,
    best_ask_price float,
    best_ask_amount float,
    ask_iv float,
    currency varchar(10),
    maturity varchar(7),
    strike varchar(8),
    type varchar(5),
    createdAt timestamp default now()
);
  COMMIT;
EOSQL