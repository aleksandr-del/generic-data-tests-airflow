{%- for col_name in params.column_names %}
SELECT
    '{{ col_name }}' AS column_name,
    {{ col_name }} AS column_value
FROM {{ params.table_name }}
WHERE
    {{ col_name }} IS NOT NULL
    AND (
        {%- for pattern in params.patterns %}
        {{ col_name }} ~ '{{ pattern }}' {% if not loop.last %}OR{% endif %}
        {%- endfor %}
    )
{% if not loop.last %}UNION ALL{% endif %}
{%- endfor %}
