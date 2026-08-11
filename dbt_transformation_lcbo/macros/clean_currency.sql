
{% macro clean_currency(column) %}

    case
        when regexp_replace(trim(both '"' from trim({{ column }})), '[$,\s]', '', 'g') ~ '^-?\d+(\.\d+)?$'
            then regexp_replace(trim(both '"' from trim({{ column }})), '[$,\s]', '', 'g')::numeric
        else null
    end
    
{% endmacro %}