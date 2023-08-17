from copy import copy
from logging import Logger
from typing import Dict, List, Optional, Any
# from os import sep

from airflow import DAG
from airflow.models import Variable, BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.utils.task_group import TaskGroup
import inspect


sep = "\\"


def specific_kwargs(**kwargs) -> dict:
    new_kwargs = {}
    specific_args_keys = inspect.getfullargspec(DbtTaskGenerator.__init__).args
    for arg_key, arg_value in kwargs.items():
        if arg_key in specific_args_keys:
            new_kwargs[arg_key] = arg_value
    return new_kwargs


def airflow_kwargs(**kwargs) -> dict:
    new_kwargs = {}
    non_airflow_kwargs = specific_kwargs(**kwargs)
    for arg_key, arg_value in kwargs.items():
        if arg_key not in non_airflow_kwargs:
            new_kwargs[arg_key] = arg_value
    return new_kwargs


class DbtNode:
    def __init__(self, full_name: str, children: List[str], config: Optional[dict], path: str):
        self.full_name = full_name
        self.children = children
        self.is_model = self.full_name.startswith('model')
        self.name = self.full_name.split('.')[-1]
        self.is_persisted = self.is_model and config["materialized"] in ['table', 'incremental', 'view']
        self.path = path


class DbtTaskGenerator:
    def __init__(
        self, dag: DAG, manifest: dict, path_model_filter: str = None
    ) -> None:
        self.dag: DAG = dag
        self.path_model_filter = path_model_filter
        self.manifest = manifest
        self.persisted_node_map: Dict[str, DbtNode] = self._get_persisted_parent_to_child_map()
        self.task_group = TaskGroup(dag=dag, group_id="dbt_group", tooltip="")

    def path_filter(self, path: str) -> bool:
        if not self.path_model_filter:
            return True
        filter_set = set(self.path_model_filter.split(sep))
        path_set = set(path.split(sep)[0:len(filter_set)])
        return filter_set == path_set

    def _get_persisted_parent_to_child_map(self) -> Dict[str, DbtNode]:
        node_info = self.manifest['nodes']
        parent_to_child_map = self.manifest['child_map']

        all_nodes: Dict[str, DbtNode] = {
            node_name: DbtNode(
                full_name=node_name,
                children=children,
                config=node_info.get(node_name, {}).get('config'),
                path=node_info[node_name]['original_file_path']
            )
            for node_name, children in parent_to_child_map.items()
        }

        persisted_nodes = {
            node.full_name: DbtNode(
                full_name=node.full_name,
                children=self._get_persisted_children(node, all_nodes),
                config=node_info.get(node_name, {}).get('config'),
                path=node_info[node_name]['original_file_path']
            )
            for node_name, node in all_nodes.items()
            if node.is_persisted and node.full_name and self.path_filter(node_info[node_name]['original_file_path'])
        }

        return persisted_nodes

    @classmethod
    def _get_persisted_children(cls, node: DbtNode, all_nodes: Dict[str, DbtNode]) -> List[str]:
        persisted_children = []
        for child_key in node.children:
            child_node = all_nodes[child_key]
            if child_node.is_persisted:
                persisted_children.append(child_key)
            else:
                persisted_children += cls._get_persisted_children(child_node, all_nodes)

        return persisted_children

    def add_all_tasks(self) -> TaskGroup:
        nodes_to_add: Dict[str, DbtNode] = {}
        for node in self.persisted_node_map:
            included_node = copy(self.persisted_node_map[node])
            included_children = []
            for child in self.persisted_node_map[node].children:
                included_children.append(child)
            included_node.children = included_children
            nodes_to_add[node] = included_node

        self._add_tasks(nodes_to_add)
        return self.task_group

    def _add_tasks(self, nodes_to_add: Dict[str, DbtNode]) -> None:
        dbt_model_tasks = self._create_dbt_run_model_tasks(nodes_to_add)

        for parent_node in nodes_to_add.values():
            if parent_node.is_model:
                self._add_model_dependencies(dbt_model_tasks, parent_node)

    def _create_dbt_run_model_tasks(self, nodes_to_add: Dict[str, DbtNode]) -> Dict[str, BaseOperator]:
        dbt_model_tasks: Dict[str, BaseOperator] = {
            node.full_name: self._create_dbt_run_task(node)
            for node in nodes_to_add.values() if node.is_model
        }
        return dbt_model_tasks

    def _create_dbt_run_task(self, node: DbtNode) -> BaseOperator:
        # This is where you create a task to run the model - see
        # https://docs.getdbt.com/docs/running-a-dbt-project/running-dbt-in-production#using-airflow
        # We pass the run date into our models: f'dbt run --models={model_name} --vars '{"run_date":""}'
        # return DummyOperator(dag=self.dag, task_id=model_name, run_date='')
        bash_command = f"dbt run --select {node.path}"
        return BashOperator(dag=self.dag,
                            task_group=self.task_group,
                            task_id=node.name,
                            bash_command=f"echo {bash_command}")

    @staticmethod
    def _add_model_dependencies(dbt_model_tasks: Dict[str, BaseOperator], parent_node: DbtNode) -> None:
        for child_key in parent_node.children:
            child = dbt_model_tasks.get(child_key)
            if child:
                dbt_model_tasks[parent_node.full_name] >> child
                # print(f"{dbt_model_tasks[parent_node.full_name].bash_command} --> {child.task_id}")
