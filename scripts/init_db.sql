CREATE DATABASE data_ranger;

\c data_ranger

CREATE TABLE IF NOT EXISTS data (
    id SERIAL PRIMARY KEY,
    timestamp DATE,
    price INT,
    user_id INT
);


CREATE TABLE IF NOT EXISTS statistics (
    id SERIAL PRIMARY KEY,
    filename TEXT,
    total_rows INT,
    avg_price INT,
    min_price INT,
    max_price INT
);
