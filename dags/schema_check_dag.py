from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from test_operators.data_test_operator import SchemaCheckTestOperator

DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "queries"
default_args = {"owner": "avdel", "retries": 0, "retry_delays": timedelta(seconds=5)}

with DAG(
    dag_id="schema_check_data_test",
    default_args=default_args,
    start_date=datetime(2026, 2, 5),
    schedule_interval="@once",
    tags=["data", "test", "schema check"],
    template_searchpath=[str(SQL_DIR)],
) as dag:
    start, end = (EmptyOperator(task_id=task) for task in "start end".split())

    tables: list[str] = "shippers shippers".split()
    schemas: list[dict[str, str]] = [
        {
            "shipper_id": "integer",
            "company_name": "varchar",
            "phone": "character varying",
        },
        {
            "shipper_id": "smallint",
            "company_name": "character varying",
            "phone": "character varying",
        },
    ]

    tasks = [
        SchemaCheckTestOperator(
            task_id=f"schema_check_{table}_{schema['shipper_id']}",
            postgres_conn_id="postgres_conn_id",
            table_name=table,
            schema=schema,
        )
        for table, schema in zip(tables, schemas)
    ]

    chain(start, tasks, end)
