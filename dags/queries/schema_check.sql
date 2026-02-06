WITH expected_schema AS (
    {%- for col_name, dtype in params.schema.items() %}
    SELECT
        '{{ col_name }}' as col_name,
        '{{ dtype }}' as dtype
    {%- if not loop.last %}UNION ALL{% endif %}
    {%- endfor %}
),

actual_schema AS (
    SELECT
        column_name AS col_name,
        data_type AS dtype
    FROM information_schema.columns
    WHERE
        table_schema = 'public'
        AND table_name = '{{ params.table_name }}'
)

SELECT * FROM expected_schema
EXCEPT
SELECT * FROM actual_schema
;
