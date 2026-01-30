SELECT
    1
FROM {{ params.table_name }}
WHERE NOT ({{ params.expression }})
LIMIT 1
