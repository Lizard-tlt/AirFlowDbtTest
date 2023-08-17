from airflow import DAG

from datetime import datetime

from dbt_task_generator import DbtTaskGroup
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


# def generate_tasks(dag):
#     import os
#     import json
#     HOME = os.getcwd()
#     # path to your dbt project
#     dbt_path = "dags"
#     manifest_path = os.path.join(HOME, dbt_path, "manifest.json")
#     with open(manifest_path) as f:  # Open manifest.json
#         manifest = json.load(f)  # Load its contents into a Python Dictionary

#     # dbt_task_generator = DbtTaskGenerator(dag, manifest, path_model_filter="models\\example")
#     dbt_task_generator = DbtTaskGenerator(dag, manifest, path_model_filter="models\\sp_web_rep_picking_base")
#     return dbt_task_generator.add_all_tasks()

def get_manifest() -> dict:
    import os
    import json
    HOME = os.getcwd()
    # path to your dbt project
    dbt_path = "dags"
    manifest_path = os.path.join(HOME, dbt_path, "manifest.json")
    with open(manifest_path) as f:  # Open manifest.json
        manifest = json.load(f)  # Load its contents into a Python Dictionary
    return manifest


with DAG("dbt_dag",
         start_date=datetime(2021, 1, 1),
         schedule="@daily",
         catchup=False,
         tags=["anatoly"]) as dag:

    task_begin_dbt = EmptyOperator(task_id="begin-dbt")
    task_end_dbt = EmptyOperator(task_id="end-dbt")

    dbt_group = DbtTaskGroup(
        manifest=get_manifest(),
        path_model_filter="models\\sp_web_rep_picking_base"
    )

    task_begin_dbt >> dbt_group >> task_end_dbt
