{% macro export_table_to_parquet(table_name, output_path) %}
    {% set query %}
        COPY (SELECT * FROM {{ ref(table_name) }}) TO '{{ output_path }}' (FORMAT PARQUET);
    {% endset %}
    {% do run_query(query) %}
    {{ log("Exported " ~ table_name ~ " to " ~ output_path, info=true) }}
{% endmacro %}