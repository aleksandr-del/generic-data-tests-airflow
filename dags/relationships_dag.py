from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import RelationshipsTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="relationships_data_test",
    default_args=default_args,
    start_date=datetime(2026, 2, 3),
    schedule_interval="@once",
    tags=["data", "test", "relationships"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    from_tables: list[str] = "order_details orders employees".split()
    from_columns: list[str] = "product_id customer_id reports_to".split()
    to_tables: list[str] = "products customers employees".split()
    to_columns: list[str] = "product_id customer_id employee_id".split()

    tasks = [
        RelationshipsTestOperator(
            task_id=f"relationships_{from_table}_{from_column}",
            postgres_conn_id="postgres_conn_id",
            table_name=from_table,
            from_column=from_column,
            to_table=to_table,
            to_column=to_column,
        )
        for from_table, from_column, to_table, to_column in zip(
            from_tables, from_columns, to_tables, to_columns
        )
    ]

    chain(start, tasks, end)
