{% macro test_date_refreshness(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

    with final as (
    select cast(max({{ column_name }}) as date) as {{ column_name }}
    from {{ model }}
    )
    select *
    from final
    where {{ column_name }} != current_date

{% endmacro %}