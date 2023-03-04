{% materialization table_static, adapter = 'bigquery' %}

    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                  schema=schema,
                                                  database=database,
                                                  type='table') -%}

    {{sql}}

    {% call statement('main') -%}
        select 1
    {%- endcall %}

    {{ return({'relations': [target_relation]})}}

{% endmaterialization %}