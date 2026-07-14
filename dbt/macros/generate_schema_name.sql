{#
  dev/prod dataset routing. The folder-level +schema gives the layer name
  (silver|gold); this macro combines it with the target to land models in:
    prod, silver -> holdit_silver        dev, silver -> holdit_silver_dev
    prod, gold   -> holdit_gold          dev, gold   -> holdit_gold_dev
  Both targets read the same Bronze; only the outputs are separated, so a broken
  model in dev can never reach the dashboard.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        holdit_{{ custom_schema_name | trim }}
    {%- else -%}
        holdit_{{ custom_schema_name | trim }}_{{ target.name }}
    {%- endif -%}
{%- endmacro %}
