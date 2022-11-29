import os
import json
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

# create Snowpark session
session = Session.builder.configs(connection_parameters).create()
# query Snowflake database and print the result
df = session.sql(os.environ['query'])
print(df.collect())

# write result to XComs folder that was created in the Dockerfile
return_json = {"return_value": str({df.collect()[0]})}
json_object = json.dumps(return_json, indent=4)

f = open('./airflow/xcom/return.json', 'w')
f.write(json_object)
f.close()

# close Snowpark session
session.close()

