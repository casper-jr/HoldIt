{#
  Every ratio in fct_metrics goes through this: a divide that yields NULL rather
  than erroring or fabricating a zero when the denominator is null or zero. Missing
  is NULL, never 0.0 — the single worst bug in the As-Is system.
#}
{% macro safe_divide(numerator, denominator) -%}
    case
        when ({{ denominator }}) is null or ({{ denominator }}) = 0 then null
        else ({{ numerator }}) / ({{ denominator }})
    end
{%- endmacro %}
