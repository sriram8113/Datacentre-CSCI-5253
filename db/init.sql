CREATE TABLE animal (
    animal_id VARCHAR(255) PRIMARY KEY,
    breed VARCHAR(255),
    color VARCHAR(255),
    name VARCHAR(255),
    date_of_birth DATE,
    animal_type VARCHAR(255)
);

CREATE TABLE outcome_type (
    outcome_type_id INT PRIMARY KEY,
    outcome_type VARCHAR(255)
);

CREATE TABLE outcome_events (
    outcome_event_id INT PRIMARY KEY,
    datetime TIMESTAMP,
    sex_upon_outcome VARCHAR(255),
    outcome_subtype VARCHAR(255),
    animal_id VARCHAR(255),
    outcome_type_id INT,
    FOREIGN KEY (animal_id) REFERENCES animal(animal_id),
    FOREIGN KEY (outcome_type_id) REFERENCES outcome_type(outcome_type_id)
);

CREATE TABLE fact_table (
    animal_id VARCHAR(255) NOT NULL,
    outcome_type_id INT,
    outcome_event_id INT,
    FOREIGN KEY (animal_id) REFERENCES animal(animal_id),
    FOREIGN KEY (outcome_type_id) REFERENCES outcome_type(outcome_type_id),
    FOREIGN KEY (outcome_event_id) REFERENCES outcome_events(outcome_event_id)
    
);