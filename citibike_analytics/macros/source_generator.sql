{% macro generate_citibike_sources() %}

{{
    return(codegen.generate_source(
        schema_name="citibike",
        table_names=["raw_citibike"],
        name="citibike_raw",
        generate_columns=True,
        include_schema=True,
        include_description=True
    ))
}}

{% endmacro %}