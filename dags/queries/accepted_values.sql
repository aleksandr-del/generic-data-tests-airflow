WITH all_values AS(
    SELECT
        {{ params.column_name }}
    FROM {{ params.table_name }}
    GROUP BY {{ params.column_name }}
)

SELECT
    1
FROM all_values
WHERE {{ params.column_name }} NOT IN {{ params.accepted_values }}
