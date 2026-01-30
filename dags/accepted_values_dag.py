from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import AcceptedValuesTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="accepted_values_data_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 29),
    schedule_interval="@once",
    tags=["data", "test", "accepted values"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables = "products orders employees".split()
    columns = "discontinued ship_via city".split()
    values = ((0, 1), (1, 2, 3), ("London", "Seattle", "Redmond"))

    tasks = [
        AcceptedValuesTestOperator(
            task_id=f"unique_{table}_{col}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            column_name=col,
            accepted_values=values,
        )
        for table, col, values in zip(tables, columns, values)
    ]

    chain(start, tasks, end)
