{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {# If a custom schema is provided (like 'silver'), use it exactly #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    
    {# If no custom schema is provided, use the default from profiles.yml #}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}

{%- endmacro %}