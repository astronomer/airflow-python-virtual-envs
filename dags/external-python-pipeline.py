from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='external-python-pipeline',
    start_date=pendulum.datetime(2022, 10, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['pyvenv', 'ExternalPythonOperator']
):

    @task
    def upstream_task():
        return "dog_intelligence"

    @task.external_python(
        python='/home/astro/.pyenv/versions/snowpark_env/bin/python'
    )
    def external_python_operator_task(table_name):

        # packages used within the virtual environment have to be imported here
        import os
        import pkg_resources
        from snowflake.snowpark import Session

        # connection parameters are stored as variables in .env
        connection_parameters = {
            "account": os.environ['SNOWFLAKE_ACCOUNT'],
            "user": os.environ['SNOWFLAKE_USER'],
            "password": os.environ['SNOWFLAKE_PASSWORD'],
            "role": os.environ['SNOWFLAKE_ROLE'],
            "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],
            "database": os.environ['SNOWFLAKE_DATABASE'],
            "schema": os.environ['SNOWFLAKE_SCHEMA'],
            "region": os.environ['SNOWFLAKE_REGION']
        }
        session = Session.builder.configs(connection_parameters).create()
        df = session.sql(f'select avg(reps_upper), avg(reps_lower) from {table_name};')
        result = str(df.collect()[0])
        print(result)
        session.close()
        return result

    @task
    def downstream_task(query_output):
        return "Snowpark results: " + f"{query_output}"


    downstream_task(external_python_operator_task(upstream_task()))