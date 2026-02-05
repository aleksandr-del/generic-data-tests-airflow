SELECT
    MAX({{ params.column_name }})
FROM {{ params.table_name }}
WHERE {{ params.column_name }} >= '{{ ds }}'::timestamp - INTERVAL '{{ params.count }} {{ params.period }}'
