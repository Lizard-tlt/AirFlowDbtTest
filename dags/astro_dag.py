"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# adjust for other database types
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
import os

YOUR_NAME = "<your_name>"
CONNECTION_ID = "dbtx"
DB_NAME = "db1"
SCHEMA_NAME = "db1"
MODEL_TO_QUERY = "model2"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_x"
DBT_PROJECT_PATH = "/opt/airflow//dbt_x"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/my_dbt/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_x",
    target_name="dev",
    profiles_yml_filepath="/home/airflow/.dbt"
    # profile_mapping=PostgresUserPasswordProfileMapping(
    #     conn_id=CONNECTION_ID,
    #     profile_args={"schema": SCHEMA_NAME},
    # ),
)

profile_config.validate_profile()
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


with DAG("dbt_dag22",
         start_date=datetime(2021, 1, 1),
         schedule="@daily",
         catchup=False,
         tags=["anatoly"]):
    pass
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"my_name": {{ params.my_name }} }',
        },
    )

    transform_data
