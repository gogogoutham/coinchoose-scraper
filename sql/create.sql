CREATE TABLE IF NOT EXISTS currency (
    symbol VARCHAR(10),
    name VARCHAR(255),
    algo VARCHAR(255),
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (symbol)
);
CREATE INDEX ON currency (name);
CREATE INDEX ON currency (algo);

CREATE TABLE IF NOT EXISTS currency_historical (
    symbol VARCHAR(10),
    name VARCHAR(255),
    algo VARCHAR(255),
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (symbol, name, algo)
);

CREATE TABLE IF NOT EXISTS network_status (
    scrape_time TIMESTAMP,
    symbol VARCHAR(10),
    current_blocks BIGINT,
    difficulty DECIMAL,
    reward DECIMAL,
    hash_rate DECIMAL,
    avg_hash_rate DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (scrape_time, symbol)
);
CREATE UNIQUE INDEX ON network_status (symbol, scrape_time);

CREATE TABLE IF NOT EXISTS network_status_latest (
    scrape_time TIMESTAMP,
    symbol VARCHAR(10),
    current_blocks BIGINT,
    difficulty DECIMAL,
    reward DECIMAL,
    hash_rate DECIMAL,
    avg_hash_rate DECIMAL,
    db_update_time TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (symbol)
);
CREATE UNIQUE INDEX ON network_status_latest (symbol, current_blocks, difficulty, hash_rate, avg_hash_rate);