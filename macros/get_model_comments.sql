
{% macro get_model_comments(target_relation,identifier) %}

  {% for node in graph.nodes.values()
     | selectattr("resource_type", "equalto", "model")
     | selectattr("name", "equalto", identifier) %}

  {% call statement('') -%}
    delete from {{database}}.{{schema}}.catalog where table='{{identifier}}'
  {%- endcall %}

  {% for column in node.columns.values() %}
  {% if column and column.description|length is defined and column.description|length > 0 %}

  {% call statement('') -%}
        alter table {{database}}.{{schema}}.{{identifier}} alter column `{{column.name}}` set options ( description = '{{column.description[:255]|replace("\":\"",":")|replace("'","\\'")}}')
  {%- endcall %}

  {% call statement('') -%}
        insert {{database}}.{{schema}}.catalog (table,column_name,description) values ('{{identifier}}','{{column.name}}','{{column.description[:255]|replace("\":\"",":")|replace("'","\\'")}}')
  {%- endcall %}

  {% endif %}
  {% endfor %}

  {% endfor %}

{% endmacro %}