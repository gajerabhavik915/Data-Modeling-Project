-- macros/float_col_conversion.sql
{% macro float_col_conversion(column) %}
    case
        when trim({{ column }}) ~ '^-?\d+(\.\d+)?$'
            then trim({{ column }})::float
        else null
    end
{% endmacro %}