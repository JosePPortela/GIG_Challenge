{% materialization table_insert, adapter = 'bigquery' %}

    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                  schema=schema,
                                                  database=database,
                                                  type='table') -%}

{% if not old_relation %}
  {% call statement('main') -%}
    create table {{target_relation}}
    as

    {{sql}}

    {% endcall -%}

    {{get_model_comments(target_relation,identifier)}}

{% else %}

  {% call statement('main') -%}
      TRUNCATE TABLE {{target_relation}}
   {% endcall -%}

  {% call statement('main') -%}
  insert {{target_relation}}

  {{sql}}

   {% endcall -%}

{% endif %}

    {% set query %}
        select  count(*)    as count_records
        from   {{target_relation}}
    {% endset %}


    {% set results = run_query(query) %}

        {% if execute %}
             {% set table_records = results.columns[0].values()[0]|int %}
        {% endif %}

    {{log("RUN:{TABLE:'"~identifier~"',TOTAL:"~table_records~"}",info=True)}}

    {% call statement('main') -%}
        select 1
    {%- endcall %}

    {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
