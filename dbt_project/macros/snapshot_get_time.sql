{% macro snapshot_get_time() %}
    -- If Airflow passes an execution_date variable, use it!
    {% if var('execution_date', none) is not none %}
        CAST('{{ var("execution_date") }}' AS TIMESTAMP)
    -- Otherwise, just use the normal real-time clock
    {% else %}
        {{ current_timestamp() }}
    {% endif %}
{% endmacro %}