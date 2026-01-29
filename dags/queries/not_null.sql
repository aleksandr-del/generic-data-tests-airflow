SELECT
    1
FROM {{ params.table_name }}
WHERE {{ params.column_name }} IS NULL
LIMIT 1
