from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import FreshnessTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="freshness_data_test",
    default_args=default_args,
    start_date=datetime(2026, 2, 4),
    schedule_interval="@once",
    tags=["data", "test", "freshness"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables: list[str] = "orders orders orders".split()
    columns: list[str] = "order_date order_date shipped_date".split()
    counts: list[str] = "1 5 5".split()
    periods: list[str] = "day day day".split()

    tasks = [
        FreshnessTestOperator(
            task_id=f"freshness_{table}_{column}_{count}_{period}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            column_name=column,
            count=count,
            period=period,
        )
        for table, column, count, period in zip(tables, columns, counts, periods)
    ]

    chain(start, tasks, end)
