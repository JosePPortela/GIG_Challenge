{{
    config(
        materialized='table_static'
    )
}}

{% if not adapter.get_relation(this.database, this.schema, this.table) %}
{% call statement('main') -%}
  create table {{database}}.{{schema}}.{{model['alias']}}
  (
  table string options (description='Table name'),
  column_name string options (description='Column name associated with the table'),
  description string options (description='Description of the column')
  )
{% endcall %}

{% for node in graph.nodes.values()
     | selectattr("resource_type", "equalto", "model") %}

{% for column in node.columns.values() %}
  {% if column.description|length is defined and column.description|length > 0 %}
    {% call statement('main') -%}
        insert {{database}}.{{schema}}.catalog (table,column_name,description) values ('{{node.name}}','{{column.name}}','{{column.description[:255]|replace("\":\"",":")|replace("'","\\'")}}')
  {%- endcall %}

  {% endif %}
  {% endfor %}
  {% endfor %}

{% endif %}