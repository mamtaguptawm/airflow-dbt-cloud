from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)

with DAG(
    dag_id="dbt_cloud_cnn_external_hourly",
    default_args={"dbt_cloud_cnn_external_test": "dbt_cloud", "account_id": 227622},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract = DummyOperator(task_id="extract")
    load = DummyOperator(task_id="load")
    dbt_test = DummyOperator(task_id="dbt_test")

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_1-cleaned",
        job_id=503176,
        check_interval=10,
        timeout=300,
    )

    extract >> load >> trigger_dbt_cloud_1-cleaned >> dbt_test
