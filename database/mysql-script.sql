CREATE DATABASE IF NOT EXISTS sink;

USE sink;

DROP TABLE IF EXISTS users;

CREATE TABLE users (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    age INT,
    operation VARCHAR(10),
    datetime_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
