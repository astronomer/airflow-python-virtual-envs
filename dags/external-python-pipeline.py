from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
import shutil
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

PYTHON = sys.executable

BASE_DIR = tempfile.gettempdir()

with DAG('py_virtual_env', schedule_interval=None, start_date=pendulum.datetime(2022, 10, 10, tz="UTC"), catchup=False, tags=['pythonvirtualenv']) as dag:
    
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    @task.external_python(task_id="external_python", python='/home/astro/.pyenv/versions/snowpark_env/bin/python')
    def callable_external_python():
        from time import sleep
        import pkg_resources
        from snowflake.snowpark import Session

        import boto3
        import json
        
        ## Checking for the correct venv packages	
        installed_packages = pkg_resources.working_set
        installed_packages_list = sorted(["%s==%s" % (i.key, i.version)
                for i in installed_packages])
        print(installed_packages_list)

        ssm = boto3.client('ssm', region_name='us-east-2')
        parameter = ssm.get_parameter(Name='/airflow/connections/snowflake/', WithDecryption=True)
        conn = json.loads(parameter['Parameter']['Value'])

        connection_parameters = {
            "account": conn['extra']['account'],
            "user": conn['login'],
            "password": conn['password'],
            "role": conn['extra']['role'],
            "warehouse": conn['extra']['warehouse'],
            "database": conn['extra']['database'],
            "schema": conn['schema'],
            "region": conn['extra']['region']
        }
        session = Session.builder.configs(connection_parameters).create()
        df = session.sql('select avg(reps_upper), avg(reps_lower) from dog_intelligence;')
        print(df)
        print(df.collect())
        session.close()

    task_print = print_context()
    task_external_python = callable_external_python()

    task_print >> task_external_python