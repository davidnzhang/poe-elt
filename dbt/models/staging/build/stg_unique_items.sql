{{ config(
    materialized='view',
    schema='staging'
    )
}}

with raw as (
select
    to_date(substr(METADATA$FILENAME,-15,10),'YYYY-MM-DD') as date,
    $1
from {{ source('staging','ext_unique_items')}}
)

select
    raw.date,
    flattened_dict.index,
    flattened_dict.value:name::varchar as name,
    flattened_dict.value:type::varchar as type
from raw, lateral flatten(input => raw.$1) flattened_dict