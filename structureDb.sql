/* docker exec -it postgres psql -U user -d mydatabase */
/* \d */
/* Get-Content structureDb.sql | docker exec -i postgres psql -U user -d mydatabase */



-- Table: activities pour stocker les activités des employés qui vont être transformés en tickets
CREATE TABLE activities (
    id SERIAL PRIMARY KEY,
    employee_id INT,
    start_timestamp TIMESTAMP NOT NULL,
    distance FLOAT,
    end_timestamp TIMESTAMP NOT NULL,
    details TEXT
);
