{#
    Macro to detect JSON schema and flatten a JSON column into individual columns

    Args:
        relation: The relation (table/view) containing the JSON column
        json_column: The name of the JSON column to flatten
        alias_prefix: Optional prefix for column aliases (defaults to json_column name)
        sample_size: The number of rows to sample from the JSON column (defaults to 1000)

    Returns:
        A list of dictionaries containing flattened column definitions with:
        - column_name: The generated column name
        - json_path: The JSON path expression
        - data_type: The data type (if detectable)

    Example usage:
        {%- set flattened_cols = flatten_json_column('user_events', 'event_data') %}
        {%- for col in flattened_cols %}
            {{ col.json_path }} as {{ col.column_name }}{% if not loop.last %},{% endif %}
        {%- endfor %}
#}

{%- macro flatten_json_column(relation, json_column, alias_prefix=none, sample_size=1000) %}

    {% if not execute %}
        {{ return([]) }}
    {% endif %}

    {%- set query_stm = "SELECT " ~ json_column ~ " FROM " ~ relation %}
    {%- call statement('get_query_result', fetch_result=True, auto_begin=false) -%}
        {{ query_stm }}
    {%- endcall -%}
    {%- set query_result = load_result('get_query_result').table.columns[0].values() | list %}

    {%- set raw_flattened_columns = modules.json_schema.infer_flattened_columns(query_result, json_column) %}
    {%- set flattened_columns = [] %}
    {#
        Expected format for each flattened column:
        {
            'column_name': 'event_data__action',
            'json_path': 'event_data.action',
            'data_type': 'text'
        }
    #}
    {%- for flattened_column in raw_flattened_columns %}
        {%- set constructed_json_path = flattened_column["json_path"].replace(".", "->>") %}
        {%- do flattened_columns.append({
            "column_name": flattened_column["column_name"],
            "json_path": constructed_json_path,
            "data_type": flattened_column["data_type"]
        }) %}
    {%- endfor %}

{%- endmacro %}
