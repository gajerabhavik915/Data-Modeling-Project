{#- Strips a trailing ".0" / ".00" from an id that came in as a float-ish string -#}

{% macro clean_item_id(column_name) -%}
    nullif(
        regexp_replace(
            trim(cast({{ column_name }} as varchar)),
            '\.0+$',
            ''
        ),
        ''
    )
{%- endmacro %}