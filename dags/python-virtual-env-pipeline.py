from airflow import DAG
from airflow.decorators import task

import logging
from pprint import pprint
import pendulum
import os

log = logging.getLogger('airflow.task')

with DAG(
    dag_id='python-virtual-env-pipeline',
    start_date=pendulum.datetime(2022, 10, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['pyvenv', 'PythonVirtualenvOperator']
):

    @task.virtualenv(
        task_id="virtualenv_python",
        python_version="3.8.14",
        requirements=[
            "snowflake-snowpark-python[pandas]",
            "boto3"
        ],
        system_site_packages=False
    )
    def python_virtual_env_operator_task():

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

    python_virtual_env_operator_task()