SELECT
    {{ params.column_name }}
FROM {{ params.table_name }}
GROUP BY {{ params.column_name }}
HAVING COUNT(*) > 1
LIMIT 1
