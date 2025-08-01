CREATE TABLE clicks (
    id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45),
    url VARCHAR(2048),
    element_id VARCHAR(255)
);

CREATE TABLE views (
    id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45),
    url VARCHAR(2048),
    duration_seconds INTEGER
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    order_id VARCHAR(255) NOT NULL UNIQUE,
    user_id VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45),
    amount DECIMAL(10, 2)
);