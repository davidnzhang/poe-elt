{{ config(
    materialized='view',
    schema='staging'
    )
}}

with raw as (
select
    to_date(substr(METADATA$FILENAME,-15,10),'YYYY-MM-DD') as date,
    $1:names::variant as names,
    $1:accounts::variant as accounts,
    $1:classes::variant as classes,
    $1:levels::variant as levels,
    $1:life::variant as life,
    $1:energyShield::variant as energyshield,
    $1:delveSolo::variant as delvesolo
from {{ source('external','ext_build_dims')}}
),

-- flatten first array (character names), use index of flattened array to lookup elements of other arrays
flattened as (
select
    raw.date,
    f.index::integer as player_id,
    f.value::varchar as character_name,
    get(raw.accounts, f.index)::varchar as account_name,
    get(raw.classes, f.index)::varchar as class,
    get(raw.levels, f.index)::integer as level,
    get(raw.life, f.index)::integer as life,
    get(raw.energyshield, f.index)::integer as energy_shield,
    get(raw.delvesolo, f.index)::integer as delve_depth
from raw,
    lateral flatten(input => raw.names) f
)

select
    {{ dbt_utils.surrogate_key(['character_name', 'account_name']) }} as character_id,
    *
from flattened

