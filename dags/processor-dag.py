import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

path_to_local_file = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_file = "MOCK_DATA.json"
dataset_url = f"https://storage.googleapis.com/datascience-public/data-eng-challenge/{dataset_file}"
output_cleaned_file = "cleaned.json"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
        dag_id="data_processing-dag",
        schedule_interval=None,
        default_args=default_args,
        catchup=True,
        max_active_runs=1,
) as dag:
    download_task = BashOperator(
        task_id="download_data",
        bash_command=f"curl -s {dataset_url} > {path_to_local_file}/{dataset_file}"

    )

    clean_data_task = BashOperator(
        task_id='clean_data',
        bash_command=f"python {path_to_local_file}/dags/processing.py clean-data --input-path \
           {path_to_local_file}/{dataset_file} --output-path {path_to_local_file}"
    )

    analyze_data_task = BashOperator(
        task_id='analyze_data',
        bash_command=f"python {path_to_local_file}/dags/processing.py analyze-data --input-path {path_to_local_file}/{output_cleaned_file} \
        --output-path {path_to_local_file}"
    )

    download_task >> clean_data_task >> analyze_data_task
