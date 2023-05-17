{{ config(materialized='table') }}

with characters as (
select
    c.date,
    c.character_id,
    c.class,
    c.level,
    c.life,
    c.energy_shield,
    c.delve_depth,
    d.main_dps_skill,
    d.dps
from {{ ref('int_characters') }} c
left join {{ ref('int_dps') }} d
    on c.date = d.date
    and c.character_id = d.character_id
)

select 
    {{ generate_date_key(date) }} as date_id,
    character_id,
    class,
    level,
    life,
    energy_shield,
    delve_depth,
    main_dps_skill,
    dps
from characters