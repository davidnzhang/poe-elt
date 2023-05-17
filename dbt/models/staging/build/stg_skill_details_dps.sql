{{ config(
    materialized='view',
    schema='staging'
    )
}}

select
    skilldetails.date,
    skilldetails.name,
    f.key::integer as player_id,
    --f.value,
    get(f.value,0)::integer as dps,
    get(f.value,1)::integer as physical_percentage,
    get(f.value,2)::integer as lightning_percentage,
    get(f.value,3)::integer as cold_percentage,
    get(f.value,4)::integer as fire_percentage,
    get(f.value,5)::integer as chaos_percentage,
    get(f.value,6)::integer as dps_type_id
from {{ source('external','ext_skill_details')}} skilldetails,
    lateral flatten(input => skilldetails.dps::variant) f