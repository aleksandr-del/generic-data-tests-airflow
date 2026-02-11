from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import BlackListTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="blacklist_patterns_data_test",
    default_args=default_args,
    start_date=datetime(2026, 2, 10),
    schedule_interval="@once",
    tags=["data", "test", "blacklist patterns"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables: list[str] = "customers employees customers".split()
    column_names: list[list[str]] = [
        ["phone"],
        ["last_name", "first_name"],
        ["contact_name"],
    ]
    patterns: list[list[str]] = [
        ["^0+$", "^[0-9]{1,5}$", ".*\?.*"],
        [".*[0-9].*"],
        ["^Mr\..*", "^Ms\..*", "^Mrs\..*"],
    ]

    tasks = [
        BlackListTestOperator(
            task_id=f"blacklist_{table}_{'_'.join(columns)}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            column_names=columns,
            patterns=patterns,
        )
        for table, columns, patterns in zip(tables, column_names, patterns)
    ]

    chain(start, tasks, end)
