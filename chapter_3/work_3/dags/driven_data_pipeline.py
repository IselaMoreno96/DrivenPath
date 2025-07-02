from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def save_raw_data():
    import pandas as pd
    from faker import Faker
    import os

    # Asegura que el directorio exista
    os.makedirs('/opt/airflow/data', exist_ok=True)

    fake = Faker()
    data = []
    for _ in range(10):  # Puedes cambiar 10 por 100 o 1000 si deseas más datos
        data.append({
            "person_name": fake.name(),
            "user_name": fake.user_name(),
            "email": fake.email(),
            "personal_number": fake.random_number(digits=8),
            "birth_date": fake.date_of_birth().isoformat(),
            "address": fake.address().replace("\n", ", "),
            "phone": fake.phone_number(),
            "mac_address": fake.mac_address(),
            "ip_address": fake.ipv4(),
            "iban": fake.iban(),
            "accessed_at": fake.date_time_this_year(),
            "session_duration": fake.random_int(min=1, max=500),
            "download_speed": fake.random_int(min=10, max=100),
            "upload_speed": fake.random_int(min=5, max=50),
            "consumed_traffic": fake.random_int(min=100, max=10000),
            "unique_id": fake.uuid4()
        })
    df = pd.DataFrame(data)

    # Guarda el CSV dentro del contenedor
    df.to_csv('/opt/airflow/data/raw_data.csv', index=False)
    print("✅ Raw data generated and saved to /opt/airflow/data/raw_data.csv")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval="* 7 * * *",
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)

create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number VARCHAR(100),
            birth_date VARCHAR(100),
            address VARCHAR(100),
            phone VARCHAR(100),
            mac_address VARCHAR(100),
            ip_address VARCHAR(100),
            iban VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration INT,
            download_speed INT,
            upload_speed INT,
            consumed_traffic INT,
            unique_id VARCHAR(100)
        );
    """,
    dag=dag,
)

load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
    COPY driven_raw.raw_batch_data(
        person_name, user_name, email, personal_number, birth_date,
        address, phone, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic,
        unique_id
    )
    FROM '/opt/airflow/data/raw_data.csv'
    DELIMITER ','
    CSV HEADER;
    """,
    dag=dag,
)

run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
    dag=dag,
)

run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
    dag=dag,
)

# Definir dependencias de forma clara
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task