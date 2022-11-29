from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='python-virtual-env-pipeline',
    start_date=pendulum.datetime(2022, 10, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['pyvenv', 'PythonVirtualenvOperator']
):

    @task
    def upstream_task():
        return "dog_intelligence"

    @task.virtualenv(
        task_id="virtualenv_python",
        python_version="3.8.14",
        requirements=[
            "snowflake-snowpark-python[pandas]"
        ],
        system_site_packages=True
    )
    def python_virtual_env_operator_task(table_name):

        # packages used within the virtual environment have to be imported here
        import os
        from snowflake.snowpark import Session

        # connection parameters are stored as variables in .env
        connection_parameters = {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "role": os.environ["SNOWFLAKE_ROLE"],
            "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
            "database": os.environ["SNOWFLAKE_DATABASE"],
            "schema": os.environ["SNOWFLAKE_SCHEMA"],
            "region": os.environ["SNOWFLAKE_REGION"]
        }

        session = Session.builder.configs(connection_parameters).create()
        df = session.sql(f"SELECT AVG(reps_upper), AVG(reps_lower)\
            FROM {table_name};")
        result = str(df.collect()[0])
        print(result)
        session.close()
        return result

    @task
    def downstream_task(query_output):
        return "Snowpark results: " + f"{query_output}"

    downstream_task(python_virtual_env_operator_task(upstream_task()))
