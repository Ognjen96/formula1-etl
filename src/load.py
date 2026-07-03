from sqlalchemy import create_engine, text
import pandas as pd
from airflow.hooks.base import BaseHook


def get_engine():
    conn = BaseHook.get_connection("sql_server_f1")
    url = f"mssql+pymssql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(url)

def sql_load(dim_tables, fact_tables):
    engine = get_engine()
    d_tables = ["DimTime","DimDriver","DimConstructor","DimCircuit","DimRace"]
    data_truncation()
    for name in d_tables: 
        dim_df = pd.read_csv(dim_tables[name])
        dim_df.to_sql(name, engine, if_exists="append", index=False)
    fact_df = pd.read_csv(fact_tables["FactResults"])
    fact_df.to_sql("FactResults", engine, if_exists="append", index=False)
    

def data_truncation():
    engine = get_engine()
    table = ["FactResults", "DimRace", "DimDriver", "DimConstructor", "DimCircuit", "DimTime"]
    with engine.connect() as conn:
        for t in table:
            conn.execute(text(f"DELETE FROM {t}"))
            conn.commit()