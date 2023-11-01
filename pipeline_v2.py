import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def extract_data():
    print('Extracting data...')
    data = pd.read_json('https://data.austintexas.gov/resource/9t4d-g238.json')
    print('Data Received')
    return data

import pandas as pd

def transform_data(data):
    print('Transforming data...')

    # Your transformation logic here
    data_new = data.copy()
    data_new.fillna('Not Recorded', inplace=True)
    data_new.columns = [col.lower() for col in data_new.columns]

    # Define the columns for each entity
    animal_columns = ['animal_id', 'breed', 'color', 'name', 'date_of_birth', 'animal_type']
    outcome_event_columns = ['outcome_event_id', 'datetime', 'sex_upon_outcome', 'outcome_subtype', 'animal_id', 'outcome_type_id']
    fact_table_columns = ['outcome_event_id', 'outcome_type_id', 'animal_id']

    # Create a unique outcome_event_id
    data_new['outcome_event_id'] = data_new.index + 1

    # Correct duplication for 'animal' and 'outcome_type' tables
    animal = data_new[animal_columns].drop_duplicates('animal_id', keep='first').reset_index(drop=True)
    unique_outcome_type = data_new[['outcome_type']].drop_duplicates().reset_index(drop=True)
    unique_outcome_type['outcome_type_id'] = unique_outcome_type.index + 1
    outcome_type = unique_outcome_type[['outcome_type_id', 'outcome_type']]

    # Map the values in the outcome_events table
    outcome_type_id_map = dict(zip(unique_outcome_type['outcome_type'], unique_outcome_type['outcome_type_id']))
    data_new['outcome_type_id'] = data_new['outcome_type'].map(outcome_type_id_map)

    # Select only the relevant columns for the outcome_events DataFrame
    outcome_events = data_new[outcome_event_columns]

    # Reset the index of outcome_events DataFrame
    outcome_events.reset_index(drop=True, inplace=True)

    fact_table = data_new[fact_table_columns]

    print('Data Transformed')
    return fact_table, animal, outcome_type, outcome_events



from sqlalchemy.exc import IntegrityError

def load_data(transformed_data):
    print('Loading data...')
    
    fact_table, animal, outcome_type, outcome_events = transformed_data

    # Define your DATABASE_URL here

    DATABASE_URL = "postgresql+psycopg2://sriram:ABCabc12345$@db:5432/shelter_hw"

    engine = create_engine(DATABASE_URL)   

    animal.to_sql('animal', engine, if_exists='append', index=False) 

    outcome_type.to_sql('outcome_type', engine, if_exists='append', index=False)
 
    outcome_events.to_sql('outcome_events', engine, if_exists='append', index=False)
   
    fact_table.to_sql('fact_table', engine, if_exists='append', index=False)
 
    
    print('Data Loading Completed')


if __name__ == '__main__':
    # Extract data
    extracted_data = extract_data()
    
    # Transform data
    transformed_data = transform_data(extracted_data)
    
    # Load data
    load_data(transformed_data)
