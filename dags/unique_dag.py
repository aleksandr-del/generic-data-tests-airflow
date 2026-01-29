from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import UniqueTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="unique_data_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 27),
    schedule_interval="@once",
    tags=["data", "test", "unique values"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables = "products employees orders".split()
    columns = "product_id employee_id customer_id".split()

    tasks = [
        UniqueTestOperator(
            task_id=f"unique_{table}_{col}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            column_name=col,
        )
        for table, col in zip(tables, columns)
    ]

    chain(start, tasks, end)
