from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import ExpressionIsTrueTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="expression_is_true_data_test",
    default_args=default_args,
    start_date=datetime(2026, 1, 29),
    schedule_interval="@once",
    tags=["data", "test", "accepted values"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables = "order_details products orders".split()
    expressions = (
        "unit_price >= 0, units_in_stock >= 0, shipped_date >= order_date".split(", ")
    )

    tasks = [
        ExpressionIsTrueTestOperator(
            task_id=f"expression_is_true_{table}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            expression=expression,
        )
        for table, expression in zip(tables, expressions)
    ]

    chain(start, tasks, end)
