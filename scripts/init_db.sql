CREATE DATABASE data_ranger;

\c data_ranger

CREATE TABLE IF NOT EXISTS data (
    timestamp DATE,
    price INT,
    user_id INT
);
