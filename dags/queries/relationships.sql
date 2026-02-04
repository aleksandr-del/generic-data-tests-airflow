WITH child AS (
    SELECT
        {{ params.column_name }} AS from_field
    FROM {{ params.table_name }}
    WHERE {{ params.column_name }} IS NOT NULL
),

parent AS (
    SELECT
        {{ params.reference_field }} AS to_field
    FROM {{ params.reference_table }}
)

SELECT
    1
FROM child
LEFT JOIN parent
    ON child.from_field = parent.to_field
WHERE parent.to_field IS NULL
