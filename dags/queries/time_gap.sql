WITH bounds AS (
    SELECT
        min({{ params.column_name }})::date AS start_date,
        max({{ params.column_name }})::date AS end_date
    FROM {{ params.table_name }}
),
expected_dates AS (
    SELECT
        generate_series(start_date, end_date, '{{ params.interval }}'::interval) AS expected_date
    FROM bounds
),
actual_dates AS (
    SELECT
        DISTINCT {{ params.column_name }}::date AS actual_date
    FROM {{ params.table_name }}
)
SELECT
    ed.expected_date
FROM expected_dates AS ed
LEFT JOIN actual_dates AS ad ON ed.expected_date = ad.actual_date
WHERE ad.actual_date IS NULL
ORDER BY 1
