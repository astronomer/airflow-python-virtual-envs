from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator
)
from kubernetes.client import models as k8s_models
from pendulum import datetime
import os

with DAG(
    dag_id="kpo-example",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["KubernetesPodOperator"]
):

    @task
    def upstream_task():
        return "dog_intelligence"

    kpo_task = KubernetesPodOperator(
        # name task and pod
        task_id="kpo_task",
        name="airflow-test-pod",
        # define the Docker image to run and env variables
        image="tjanif/docker-sandbox:snowpark-example",
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.environ["SNOWFLAKE_ACCOUNT"],
            "SNOWFLAKE_USER": os.environ["SNOWFLAKE_USER"],
            "SNOWFLAKE_PASSWORD": os.environ["SNOWFLAKE_PASSWORD"],
            "SNOWFLAKE_ROLE": os.environ["SNOWFLAKE_ROLE"],
            "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
            "SNOWFLAKE_DATABASE": os.environ["SNOWFLAKE_DATABASE"],
            "SNOWFLAKE_SCHEMA": os.environ["SNOWFLAKE_SCHEMA"],
            "SNOWFLAKE_REGION": os.environ["SNOWFLAKE_REGION"],
            "query": "SELECT AVG(reps_upper), AVG(reps_lower)\
                 FROM " + "{{ ti.xcom_pull(task_ids=['upstream_task'])[0]}};"
        },
        # define cluster and namespace to run the pod in
        namespace=conf.get('kubernetes', 'NAMESPACE'),
        in_cluster=True,
        # container resource limits
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "250M", "cpu": "100m"},
        ),
        # parameters handling logs
        get_logs=True,
        log_events_on_failure=True,
        # delete pod after task is done
        is_delete_operator_pod=True,
        do_xcom_push=True
    )

    @task
    def downstream_task(query_output):
        return "Snowpark results: " + f"{query_output}"

    upstream_task() >> kpo_task >> downstream_task(kpo_task.output)