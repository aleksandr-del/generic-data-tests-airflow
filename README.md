# Generic Data Tests with Airflow

A dbt-like data testing framework built on Apache Airflow with custom operators for validating data quality in PostgreSQL databases.

## Features

- **Custom Test Operators**: Reusable operators for common data quality checks
- **Template-based SQL**: Parameterized SQL queries for flexible testing
- **Northwind Dataset**: Pre-populated sample database for testing
- **Parallel Execution**: Leverage Airflow's task parallelization

## Test Types

### NotNullTestOperator

Validates that specified columns contain no null values.

### UniqueTestOperator

Ensures column values are unique across the table.

### AcceptedValuesTestOperator

Validates column values against a predefined list (placeholder).

## Quick Start

1. **Start Services**

```bash
docker-compose up -d
```

2. **Run Tests**
   Access Airflow UI at `http://localhost:8080` and trigger the DAGs:

- `not_null_data_test`
- `unique_data_test`

## Project Structure

```
├── dags/
│   ├── queries/           # SQL test templates
│   ├── not_null_dag.py    # Not null validation DAG
│   └── unique_dag.py      # Uniqueness validation DAG
├── plugins/
│   └── test_operators/    # Custom Airflow operators
├── init-dwh/              # Database initialization scripts
└── docker-compose.yml     # Airflow setup
```

## Usage

Create new tests by:

1. Adding SQL templates to `dags/queries/`
2. Extending base operators in `plugins/test_operators/`
3. Building DAGs that chain test tasks

## Example

```python
NotNullTestOperator(
    task_id="not_null_products_name",
    postgres_conn_id="postgres_conn_id",
    table_name="products",
    column_name="product_name"
)
```

## Requirements

- Docker & Docker Compose
- PostgreSQL
- Apache Airflow 2.x
