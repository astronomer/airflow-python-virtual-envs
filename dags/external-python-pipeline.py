from airflow import DAG
from airflow.decorators import task

import logging
from pprint import pprint
import pendulum

log = logging.getLogger('airflow.task')

with DAG(
    dag_id='external-python-pipeline',
    start_date=pendulum.datetime(2022, 10, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['pyvenv', 'ExternalPythonOperator']
):

    @task.external_python(
        python='/home/astro/.pyenv/versions/snowpark_env/bin/python'
    )
    def external_python_operator_task():

        # packages used within the virtual environment have to be imported here
        import os
        import pkg_resources
        from snowflake.snowpark import Session

        ## Checking for the correct venv packages	
        installed_packages = pkg_resources.working_set
        installed_packages_list = sorted(["%s==%s" % (i.key, i.version)
                for i in installed_packages])
        print(installed_packages_list)

        # connection parameters are stored as variables in .env
        connection_parameters = {
            "account": os.environ['SNOWFLAKE_ACCOUNT'],
            "user": os.environ['SNOWLAKE_USER'],
            "password": os.environ['SNOWFLAKE_PASSWORD'],
            "role": os.environ['SNOWFLAKE_ROLE'],
            "warehouse": os.environ['SNOWFLAKE_WAREHOUSE'],
            "database": os.environ['SNOWLAKE_DATABASE'],
            "schema": os.environ['SNOWFLAKE_SCHEMA'],
            "region": os.environ['SNOWFLAKE_REGION']
        }
        session = Session.builder.configs(connection_parameters).create()
        df = session.sql('select avg(reps_upper), avg(reps_lower) from dog_intelligence;')
        print(df)
        print(df.collect())
        session.close()

    external_python_operator_task()