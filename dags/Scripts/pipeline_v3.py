from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import requests
import io
import time
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


load_dotenv("./dags/secrets.env")



URL = os.getenv('URL')
YOUR_ACCESS_KEY = os.getenv('YOUR_ACCESS_KEY')
YOUR_SECRET_KEY = os.getenv('YOUR_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
DATABASE_URL = os.getenv('DATABASE_URL')


DATABASE_URL = DATABASE_URL.format(POSTGRES_USER,POSTGRES_PASSWORD,POSTGRES_HOST,POSTGRES_PORT,POSTGRES_DB)


def extract():
    # AWS S3 client initialization
    s3 = boto3.client(
        's3',
        aws_access_key_id=YOUR_ACCESS_KEY,
        aws_secret_access_key=YOUR_SECRET_KEY
    )

    # Delete existing data in 'raw_data' folder
    objects_to_delete = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='raw_data/')
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3.delete_objects(Bucket=BUCKET_NAME, Delete=delete_keys)

    # Retrieve new data
    df_new = pd.read_json(URL)
    df_new = df_new[['animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
       'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
       'breed', 'color', 'outcome_subtype']]

    # Check for existing data in 'old_Data' folder
    old_data_key = 'old_data/austin_animal_outcomes.csv'
    try:
        old_obj = s3.get_object(Bucket=BUCKET_NAME, Key=old_data_key)
        df_old = pd.read_csv(io.BytesIO(old_obj['Body'].read()))
    except s3.exceptions.NoSuchKey:
        df_old = pd.DataFrame(columns = ['animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
       'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
       'breed', 'color', 'outcome_subtype'])  # If no old data, create an empty DataFrame

    # Compare and get only new or updated records
    df_combined = pd.concat([df_new, df_old])
    df_combined.drop_duplicates(keep=False, inplace=True)
    df_updated = df_combined[~df_combined.index.isin(df_old.index)]

    if df_updated.empty:
        logging.info("No new or updated data to upload. Creating an empty CSV with headers.")
        df_updated = pd.DataFrame(columns=df_new.columns)

    # Upload data to 'raw_data'
    logging.info("Uploading data to 'raw_data'...")
    csv_buffer = io.StringIO()
    df_updated.to_csv(csv_buffer, index=False)
    file_content = csv_buffer.getvalue().encode()
    s3.put_object(Bucket=BUCKET_NAME, Key='raw_data/austin_animal_outcomes.csv', Body=file_content)

    # Update 'Old_Data' with current data
    logging.info("Updating 'old_data' with current data...")
    csv_buffer_old = io.StringIO()
    df_new.to_csv(csv_buffer_old, index=False)
    old_file_content = csv_buffer_old.getvalue().encode()
    s3.put_object(Bucket=BUCKET_NAME, Key=old_data_key, Body=old_file_content)





def transform():
    s3 = boto3.client(
        's3',
        aws_access_key_id=YOUR_ACCESS_KEY,
        aws_secret_access_key=YOUR_SECRET_KEY
    )
    bucket_name = BUCKET_NAME  # Replace with your S3 bucket name
    s3_object_name = 'raw_data/austin_animal_outcomes.csv'

    # Download the file from S3
    obj = s3.get_object(Bucket=bucket_name, Key=s3_object_name)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Perform transformations
    data = df.copy() # Example transformation

    # Transormations

    data.fillna('Not Recorded',inplace=True)
    data['outcome_type_id'] = data.index + 1
    data['outcome_event_id'] = data.index + 1



    # Dividing into entities 
    animal_table = ['animal_id', 'breed', 'color', 'name','date_of_birth','animal_type']
    outcome_table = ['outcome_type_id','outcome_type']
    outcome_event = ['outcome_event_id','datetime','sex_upon_outcome','outcome_subtype','animal_id','outcome_type']
    data_colums_order = ['animal_id',
            'outcome_type_id','outcome_event_id']

    data_colums_order = ['animal_id',
            'outcome_type','outcome_event_id']

    # re-ordering
    animal = data[animal_table]
    outcomes = data[outcome_table]
    outcome_events = data[outcome_event]
    data = data[data_colums_order]
   
    # Correcting Duplication
    animal.drop_duplicates(inplace=True)
    outcomes = pd.DataFrame(pd.Series(outcomes['outcome_type'].unique(),name='outcome_type'))
    outcomes['outcome_type_id'] = outcomes.index + 1 
    outcomes = outcomes[['outcome_type_id','outcome_type']]
    outcomes_2 = outcomes[['outcome_type','outcome_type_id']]
    
    dictionary_of_outcomes = dict(zip(outcomes_2['outcome_type'],outcomes_2['outcome_type_id']))
    outcome_events['outcome_type_id']= outcome_events['outcome_type'].map(dictionary_of_outcomes)

    outcome_events = outcome_events.drop('outcome_type', axis=1)


    data["outcome_type_id"] = data['outcome_type'].map(dictionary_of_outcomes)
    data = data.drop('outcome_type', axis=1)

  
    # Uploading to the cloud

    # Upload transformed data back to S3
    out_buffer = io.StringIO()
    data.drop_duplicates(inplace = True)
    data.to_csv(out_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='cleaned_data/data.csv', Body=out_buffer.getvalue())

    out_buffer = io.StringIO()
    animal.drop_duplicates(inplace = True)
    animal.to_csv(out_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='cleaned_data/animal.csv', Body=out_buffer.getvalue())
  
    out_buffer = io.StringIO()
    outcomes.drop_duplicates(inplace = True)
    outcomes.to_csv(out_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='cleaned_data/outcomes.csv', Body=out_buffer.getvalue())

    out_buffer = io.StringIO()
    outcome_events.drop_duplicates(inplace = True)
    outcome_events.to_csv(out_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key='cleaned_data/outcomes_events.csv', Body=out_buffer.getvalue())
    


# loading into Redshift

def load():

    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=YOUR_ACCESS_KEY,
        aws_secret_access_key=YOUR_SECRET_KEY
    )

    # Download data from S3 and load into DataFrames
    files = ['cleaned_data/animal.csv', 'cleaned_data/outcomes.csv', 'cleaned_data/outcomes_events.csv', 'cleaned_data/data.csv']
    dataframes = {}
    for file in files:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file)
        dataframes[file] = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Create an engine instance
    engine = create_engine(DATABASE_URL)

    # Upload DataFrames to PostgreSQL
    dataframes['cleaned_data/animal.csv'].to_sql('animal', engine, if_exists='append', index=False)
    dataframes['cleaned_data/outcomes.csv'].to_sql('outcome_type', engine, if_exists='append', index=False)
    dataframes['cleaned_data/outcomes_events.csv'].to_sql('outcome_event', engine, if_exists='append', index=False)
    dataframes['cleaned_data/data.csv'].to_sql('fact_table', engine, if_exists='append', index=False)
