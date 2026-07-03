from airflow.sdk import dag, task
import pandas as pd
from datetime import datetime
from src.extract import download_from_drive
from src.transform import load_data, clean_data, dim_creation, fact_creation
from src.load import sql_load, data_truncation
from sqlalchemy import create_engine, text

@dag(
    dag_id = "ETL-Formula1",
    description = ("Dag to define ETL process for Formula 1 dataset"),
    schedule = None,
    start_date = datetime(2024,1,1),
    catchup = False
)
def pipeline():
    
    @task(retries = 2)
    def extract():
        return download_from_drive()
    
    @task
    def transform(csv_path: str):
        df = pd.read_csv(csv_path)
        cleaned_data = clean_data(df)
        dim_tables = dim_creation(cleaned_data)
        fact_tables = fact_creation(cleaned_data)

        return {"dims": dim_tables, "fact": fact_tables}

    @task(retries = 2)
    def load(fact_and_dim: dict):
        sql_load(fact_and_dim["dims"], fact_and_dim["fact"])


    csv_path = extract()
    cleaned_data = transform(csv_path)
    load(cleaned_data)

pipeline()
