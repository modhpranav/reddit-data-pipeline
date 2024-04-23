{% macro drop_model(database,schema,table_name) %}

{% set query %}
    DROP TABLE IF EXISTS {{database}}.{{schema}}.{{table_name}}
{% endset %}

{{log(query, info=true)}}
{% do run_query(query) %}

{% endmacro %}