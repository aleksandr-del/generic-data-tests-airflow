from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import NotNullTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="not_null_data_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 27),
    schedule_interval="@once",
    tags=["data", "test", "not null"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables = "products orders employees".split()
    columns = "product_name ship_region reports_to".split()

    tasks = [
        NotNullTestOperator(
            task_id=f"not_null_{table}_{col}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            column_name=col,
        )
        for table, col in zip(tables, columns)
    ]

    chain(start, tasks, end)
