

{% macro clean_text(column) %}

    nullif(initcap(regexp_replace(trim({{ column }}), '\s+', ' ', 'g')), '')

{% endmacro %}

