 {#
    This macro creates a "smart key" from the date field 
#}

{% macro generate_date_key(date) -%}

    (left(to_char(date),4)||substr(to_char(date),6,2)||right(to_char(date),2))::integer

{%- endmacro %}