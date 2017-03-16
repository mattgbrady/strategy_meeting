--PG Schema for prototype market and econ database
--
--Tables for sources, query specification, and raw data

DROP TABLE IF EXISTS tickers CASCADE;
CREATE TABLE tickers
    (id            SERIAL PRIMARY KEY,
    data_source    TEXT NOT NULL,
    ticker         TEXT NOT NULL,
    field          TEXT,
    long_name      TEXT,
    short_name     TEXT,
    category       TEXT,
    geography      TEXT,
    sector         TEXT,
    ticker_type    TEXT,
    calc_returns   BOOLEAN,
    perf_tbl_bucket_one TEXT,
    perf_tbl_bucket_two TEXT,
    UNIQUE (data_source, ticker,field),
    created_at     TIMESTAMP NOT NULL DEFAULT now()
  );

DROP TABLE IF EXISTS raw_data CASCADE;
CREATE TABLE raw_data
    (id           SERIAL PRIMARY KEY,
    ticker_id     INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date      TIMESTAMP NULL,
    mkt_value     NUMERIC NULL,
    mkt_text      TEXT NULL,
    UNIQUE (ticker_id, mkt_date, mkt_value),
    created_at    TIMESTAMP NOT NULL DEFAULT now()
  );

DROP TABLE IF EXISTS returns_to_date CASCADE;
CREATE TABLE returns_to_date
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    short_name TEXT NOT NULL,
    to_date_return numeric,
    return_type TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );


DROP TABLE IF EXISTS cumulative_returns CASCADE;
CREATE TABLE cumulative_returns
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    short_name TEXT NOT NULL,
    cumulative_return numeric NOT NULL,
    return_type TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );

DROP TABLE IF EXISTS cumulative_std_return CASCADE;
CREATE TABLE cumulative_std_return
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    short_name TEXT NOT NULL,
    cumulative_std_return numeric NOT NULL,
    std_type TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );

CREATE OR REPLACE VIEW data_ticker_id_join AS
SELECT ticker_id, ticker, field, short_name, mkt_date::timestamp::date, mkt_value, mkt_text FROM (
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY (ticker_id, mkt_date) ORDER BY created_at DESC) col FROM raw_data AS T1) AS T2 INNER JOIN tickers ON (ticker_id = tickers.id) WHERE col = 1;

CREATE OR REPLACE VIEW to_date_join_view AS
SELECT ticker_id, returns_to_date.short_name, perf_tbl_bucket_one, perf_tbl_bucket_two, to_date_return, return_type FROM returns_to_date INNER JOIN tickers ON returns_to_date.ticker_id = tickers.id;

DROP TABLE IF EXISTS rolling_volatility_std CASCADE;
CREATE TABLE rolling_volatility_std
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    short_name TEXT NOT NULL,
    vol_std numeric NOT NULL,
    std_type TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );

DROP TABLE IF EXISTS rolling_volatility CASCADE;
CREATE TABLE rolling_volatility
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    short_name TEXT NOT NULL,
    rolling_vol numeric NOT NULL,
    std_type TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );

DROP TABLE IF EXISTS rolling_corr CASCADE;
CREATE TABLE rolling_corr
    (id      SERIAL PRIMARY KEY,
    mkt_date TIMESTAMP NOT NULL,
    correlation numeric NOT NULL,
    short_name_one TEXT NOT NULL,
    short_name_two TEXT NOT NULL,
    time_period TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );
DROP TABLE IF EXISTS rolling_corr_std CASCADE;
CREATE TABLE rolling_corr_std
    (id      SERIAL PRIMARY KEY,
    mkt_date TIMESTAMP NOT NULL,
    correlation_std numeric NOT NULL,
    short_name_one TEXT NOT NULL,
    short_name_two TEXT NOT NULL,
    time_period TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );


DROP TABLE IF EXISTS trend_probability CASCADE;
CREATE TABLE trend_probability
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    prob_trend_change numeric NOT NULL,
    short_name TEXT NOT NULL,
    time_period TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );

DROP TABLE IF EXISTS rolling_tr CASCADE;
CREATE TABLE rolling_tr
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    rolling_tr numeric NOT NULL,
    short_name TEXT NOT NULL,
    time_period TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );

DROP TABLE IF EXISTS rolling_trend CASCADE;
CREATE TABLE rolling_trend
    (id      SERIAL PRIMARY KEY,
    ticker_id INTEGER NOT NULL REFERENCES tickers(id),
    mkt_date TIMESTAMP NOT NULL,
    rolling_trend numeric NOT NULL,
    short_name TEXT NOT NULL,
    time_period TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
    );