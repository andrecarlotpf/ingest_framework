from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'start_date': '2025-05-03 15:24:03.365828',
    'retries': 0
}

with DAG(
    dag_id='example_dag',
    schedule_interval='None',
    default_args=default_args,
    catchup=False,
    description= 'Example DAG generated from template',
    tags=['bigquery', 'docker', 'airflow']
) as dag:

    run_container = DockerOperator(
        task_id='ingest_stately-lodge-369614.gcp_dev.mercantil_framework',
        image='ingest_framework:latest', 
        api_version='auto',
        auto_remove=True,
        command=None,
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False,
        environment={'source': 'bigquery', 'table_name': 'stately-lodge-369614.gcp_dev.mercantil_framework', 'primary_key': 'id', 'incremental_field': 'disponibilidade', 'tags': ['docker', 'airflow'], 'columns': {'id': 'STRING', 'nome': 'STRING', 'disponibilidade': 'STRING'}},
    )

run_container