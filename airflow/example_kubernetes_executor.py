"""
Reference: https://github.com/apache/airflow/blob/main/airflow/example_dags/example_local_kubernetes_executor.py
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

try:
    from kubernetes.client import models as k8s
except ImportError:
    log.warning("Could not import DAGs in example_local_kubernetes_executor.py", exc_info=True)
    log.warning("Install Kubernetes dependencies with: pip install apache-airflow[cncf.kubernetes]")
    k8s = None

if k8s:
    with DAG(
        dag_id="tom_example_kubernetes_executor",
        schedule=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=["example"],
    ) as dag:

        @task(task_id="task_with_executor")
        def task_with_executor(ds=None, **kwargs):
            """Print the Airflow context and ds variable from the context."""
            print(kwargs)
            print(ds)
            return "Whatever you return gets printed in the logs"

        EmptyOperator(task_id="start") >> task_with_executor() >> EmptyOperator(task_id="end")
