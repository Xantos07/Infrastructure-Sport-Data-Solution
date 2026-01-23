
CREATE TABLE activities (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    start_timestamp TIMESTAMP NOT NULL,
    distance FLOAT,
    end_timestamp TIMESTAMP NOT NULL,
    details TEXT
);
