

{% macro parse_multi_format_date(column) %}

case
    when trim({{ column }}) ~ '^\d{4}-\d{2}-\d{2}$'
        then to_date(trim({{ column }}), 'YYYY-MM-DD')
    when trim({{ column }}) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
        then to_date(trim({{ column }}), 'MM/DD/YYYY')
    when trim({{ column }}) ~ '^\d{1,2}-[A-Za-z]{3}-\d{4}$'
        then to_date(trim({{ column }}), 'DD-Mon-YYYY')
    when trim({{ column }}) ~ '^\d{1,2}-[A-Za-z]{3}-\d{4}$'
        then to_date(trim({{ column }}), 'DD-Mon-YY')
    else null
end

{% endmacro %}