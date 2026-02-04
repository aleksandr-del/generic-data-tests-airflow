WITH child AS (
    SELECT
        {{ params.from_column }} AS from_field
    FROM {{ params.table_name }}
    WHERE {{ params.from_column }} IS NOT NULL
),

parent AS (
    SELECT
        {{ params.to_column }} AS to_field
    FROM {{ params.to_table }}
)

SELECT
    1
FROM child
LEFT JOIN parent
    ON child.from_field = parent.to_field
WHERE parent.to_field IS NULL
