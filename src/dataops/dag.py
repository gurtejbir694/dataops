from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dataops.data import generate_synthetic_data
from dataops.quality import run_quality_checks
from dataops.alert import send_alert
from dataops.config import load_config

config = load_config()

with DAG(
    "data_quality_dag",
    start_date=datetime(2025, 6, 12),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate_synthetic_data,
        op_kwargs={"n": 100, "config": config, "verbose": True},
    )
    
    quality_task = PythonOperator(
        task_id="check_quality",
        python_callable=run_quality_checks,
        op_kwargs={"source": "db", "csv_path": None, "config": config, "verbose": True},
    )
    
    alert_task = PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
        op_kwargs={"subject": "Data Quality Report", "body": "Quality checks completed", "config": config, "verbose": True},
    )
    
    generate_task >> quality_task >> alert_task
