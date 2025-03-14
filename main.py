import pandas as pd
import duckdb
import os

duckdb_path = "/home/jd/airflow-dbt/dbt_project/p1.duckdb"

# Connect to DuckDB
with duckdb.connect(duckdb_path) as con:
    try:


        tables = ["e_history_min","e_history_uur","e_history_dag","e_history_maand","e_history_jaar"]
        query = """
        select * from e_history_min order by timestamp desc limit 1001;
        """
        df = con.sql(query).df()
        print(df.head())
    except Exception as e:
        print(f"An error occurred: {e}")

    # finally:
print("Process completed.")
