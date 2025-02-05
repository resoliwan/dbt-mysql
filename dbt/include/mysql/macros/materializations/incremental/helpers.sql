
{% macro incremental_delete(tmp_relation, target_relation, unique_key=none, statement_name="pre_main") %}
    {% if unique_key is not none %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target_relation }}
            using {{ tmp_relation }}
            where (
                {% for key in unique_key %}
                    {{ tmp_relation }}.{{ key }} = {{ target_relation }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
            );
        {% else %}
            delete from {{ target_relation }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ tmp_relation }}
            );
        {% endif %}
    {% endif %}
{%- endmacro %}

{% macro incremental_insert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    )
{%- endmacro %}
