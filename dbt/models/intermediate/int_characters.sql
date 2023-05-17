{{ config(materialized='table', transient=true)}}

select
    b.date,
    b.character_id,
    get(c.value::array, b.class::integer)::varchar as class,
    b.level,
    b.life,
    b.energy_shield,
    b.delve_depth
from {{ ref("stg_build_dims") }} b
join {{ source('external','ext_class_names')}} c
    on b.date = c.date

-- note: dps and main dps skill info to be added: need to think of ways to optimize queries as current method of joining by date & character key is taking >20 mins using XS warehouse