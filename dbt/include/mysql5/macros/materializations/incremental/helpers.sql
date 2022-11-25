
{% macro incremental_delete(tmp_relation, target_relation, unique_key=none, statement_name="pre_main") %}
    {% if unique_key is not none %}
        {% if unique_key is sequence and unique_key is not string %}
            delete {{target_relation }}
            from {{target_relation }}, {{ tmp_relation }}
            where
                {% for key in unique_key %}
                    {{ tmp_relation }}.{{ key }} = {{ target_relation }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
            ;
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
    {%- set on_dups_update_cols = config.get('on_dups_update_cols') -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    )

    {% if on_dups_update_cols is not none %}
        on duplicate key update
        {% for key in on_dups_update_cols %}
            {{ key }} = {{ tmp_relation }}.{{ key }}
            {{ "," if not loop.last }}
        {% endfor %}
    {% endif %}

{%- endmacro %}
