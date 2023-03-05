{% macro test_model_empty(model) %}

    with final as (
    select count(*) as count_records
    from {{ model }}
    )
    select *
    from final
    where count_records=0

{% endmacro %}