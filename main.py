# We'll start by importing the DAG object
from datetime import timedelta
from doctest import master

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os

# get dag directory path
dag_path = os.getcwd()


def transform_data():
    task = pd.read_csv(f"{dag_path}/raw_data/tabel_task.csv", low_memory=False)
    master = pd.read_csv(f"{dag_path}/raw_data/tabel_master.csv", low_memory=False)
    

    # merge booking with client
    data = pd.merge(task, master, on='n_nik')
    


    

    # load processed data
    data.to_csv(f"{dag_path}/processed_data/relasi_data.csv", index=False)


def load_data():
    conn = sqlite3.connect("/usr/local/airflow/db/dataanalyst.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS relasi_record (
                    task_id INTEGER NOT NULL,
                    n_nik INTEGER NOT NULL,
                    create_time DATE,
                    update_time DATE,
                    description TEXT,
                    status INTEGER,
                    v_nama_karyawan TEXT,
                    v_short_posisi TEXT(512),
                    c_company_code INTEGER,
                    c_kode_divisi TEXT(512),
                    c_kode_unit TEXT,
                    v_band_posisi TEXT(512),
                    c_employee_group INTEGER,
                    c_employee_subgroup INTEGER
                );
             ''')
    records = pd.read_csv(f"{dag_path}/processed_data/relasi_data.csv")
    records.to_sql('relasi_record', conn, if_exists='replace', index=False)


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'Relasi_karyawan',
    default_args=default_args,
    description='karyawan for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)


task_1 >> task_2