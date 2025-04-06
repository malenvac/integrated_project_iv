import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# Asegura que se pueda importar desde tu carpeta src
sys.path.append("/opt/airflow/integrated_project/src")

from config import DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL, SQLITE_BD_ABSOLUTE_PATH, get_csv_to_table_mapping
from extract import extract
from load import load
from transform import run_queries

DB_URL = f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}"

def extract_task(ti):
    print("🚀 Iniciando tarea de extracción...")
    print(f"📂 DATASET_ROOT_PATH: {DATASET_ROOT_PATH}")
    print(f"🔗 PUBLIC_HOLIDAYS_URL: {PUBLIC_HOLIDAYS_URL}")

    try:
        mapping = get_csv_to_table_mapping()
        print(f"📑 CSV → Tabla mapping: {mapping}")

        dataframes = extract(DATASET_ROOT_PATH, mapping, PUBLIC_HOLIDAYS_URL)
        print("✅ Extracción completada. DataFrames extraídos:")
        for key, df in dataframes.items():
            print(f"📄 {key}: {df.shape} filas")

        ti.xcom_push(key="dataframes", value=dataframes)
    except Exception as e:
        print(f"❌ Error en extract_task: {e}")
        raise

def load_task(ti):
    print("🚚 Iniciando tarea de carga a la base de datos...")
    print(f"🔗 Conectando a la base de datos: {DB_URL}")
    
    try:
        engine = create_engine(DB_URL)
        dataframes = ti.xcom_pull(task_ids="extract", key="dataframes")

        if not dataframes:
            print("⚠️ No se encontraron DataFrames en XCom.")
            raise ValueError("No se recibió ningún dataframe desde extract_task.")
        
        load(dataframes, engine)
        print("✅ Carga completada exitosamente.")
    except Exception as e:
        print(f"❌ Error en load_task: {e}")
        raise

def transform_task():
    print("🔧 Iniciando tarea de transformación...")
    print(f"🔗 Conectando a la base de datos: {DB_URL}")

    try:
        engine = create_engine(DB_URL)
        query_results = run_queries(engine)

        print("✅ Transformaciones ejecutadas. Resultados:")
        for name, df in query_results.items():
            print(f"\n🔍 Resultado para: {name}")
            print(df.head())
    except Exception as e:
        print(f"❌ Error en transform_task: {e}")
        raise

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_olist_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["olist", "etl"],
) as dag:

    extract_op = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    load_op = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    transform_op = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    extract_op >> load_op >> transform_op
