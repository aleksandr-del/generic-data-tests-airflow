# Generic Data Tests with Airflow

A dbt-like data testing framework built on Apache Airflow with custom operators for validating data quality in PostgreSQL databases.

## Features

- **Custom Test Operators**: Reusable operators for common data quality checks
- **Template-based SQL**: Parameterized SQL queries for flexible testing
- **Northwind Dataset**: Pre-populated sample database for testing
- **Parallel Execution**: Leverage Airflow's task parallelization

## Sample Data Updates

The orders table has been updated with realistic date ranges for testing:

```sql
-- Set order_date to 3-13 days ago
UPDATE orders SET order_date = (current_date - ceiling(random() * 10 + 3) * interval '1 day')::date;

-- Set required_date to 10 days after order_date
UPDATE orders SET required_date = (order_date + interval '10 day')::date;

-- Set shipped_date to 1-4 days after order_date
UPDATE orders SET shipped_date = (order_date + ceiling((random() * 10 + 1) / 3) * interval '1 day')::date;
```

## Test Types

### NotNullTestOperator

Validates that specified columns contain no null values.

### UniqueTestOperator

Ensures column values are unique across the table.

### AcceptedValuesTestOperator

Validates column values against a predefined list of acceptable values.

### ExpressionIsTrueTestOperator

Tests custom SQL expressions for business rule validation (e.g., `unit_price >= 0`).

### RelationshipsTestOperator

Validates referential integrity between tables by checking foreign key relationships.

### FreshnessTestOperator

Validates data freshness by checking if records exist within a specified time window from the execution date. Uses Jinja's `{{ ds }}` template variable making tests idempotent across reruns.

## Quick Start

1. **Start Services**

```bash
docker-compose up -d
```

2. **Run Tests**
   Access Airflow UI at `http://localhost:8080` and trigger the DAGs:

- `not_null_data_test`
- `unique_data_test`
- `accepted_values_data_test`
- `expression_is_true_data_test`
- `relationships_data_test`
- `freshness_data_test`

## Project Structure

```
├── dags/
│   ├── queries/                  # SQL test templates
│   ├── not_null_dag.py           # Not null validation DAG
│   ├── unique_dag.py             # Uniqueness validation DAG
│   ├── accepted_values_dag.py    # Accepted values validation DAG
│   ├── expression_is_true_dag.py # Expression validation DAG
│   ├── relationships_dag.py      # Relationships validation DAG
│   └── freshness_dag.py          # Freshness validation DAG
├── plugins/
│   └── test_operators/           # Custom Airflow operators
├── init-dwh/                     # Database initialization scripts
└── docker-compose.yml            # Airflow setup
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

RelationshipsTestOperator(
    task_id="relationships_orders_customer",
    postgres_conn_id="postgres_conn_id",
    table_name="orders",
    from_column="customer_id",
    to_table="customers",
    to_column="customer_id"
)

FreshnessTestOperator(
    task_id="freshness_orders_order_date",
    postgres_conn_id="postgres_conn_id",
    table_name="orders",
    column_name="order_date",
    count="1",
    period="day"
)
```

## Requirements

- Docker & Docker Compose
- PostgreSQL
- Apache Airflow 2.x
