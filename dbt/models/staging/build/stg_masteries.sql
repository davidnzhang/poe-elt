{{ config(
    materialized='view',
    schema='staging'
    )
}}

with raw as (
select
    to_date(substr(METADATA$FILENAME,-15,10),'YYYY-MM-DD') as date,
    $1
from {{ source('external','ext_masteries')}}
)

select
    raw.date,
    flattened_dict.index,
    flattened_dict.value:name::varchar as name
from raw, lateral flatten(input => raw.$1) flattened_dict