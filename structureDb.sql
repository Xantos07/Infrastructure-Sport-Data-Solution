/* docker exec -it postgres psql -U user -d mydatabase */
/* \d */
/* Get-Content structureDb.sql | docker exec -i postgres psql -U user -d mydatabase */

-- Table: activities pour stocker les activités des employés qui vont être transformés en tickets
CREATE TABLE activities (
    id SERIAL PRIMARY KEY,
    employee_id INT NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    sport_type VARCHAR(50) NOT NULL,
    distance INT,  -- en mètres
    elapsed_time INT NOT NULL,  -- en secondes
    details TEXT
);