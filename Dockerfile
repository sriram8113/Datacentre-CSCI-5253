FROM airflow:latest

WORKDIR /app

COPY dags/DAG.py dags/DAG.py

COPY dags/Scripts/etl.py dags/Scripts/etl.py

COPY dags/secrets.env dags/secrets.env

RUN pip install pandas sqlalchemy psycopg2 python-dotenv apache-airflow boto3 io logging

ENTRYPOINT [ "bash" ]