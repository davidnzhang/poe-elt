{{ config(materialized='incremental') }}

-- this is configured as an incremental model as only newly ingested data (as defined by the period grain i.e. latest date) need to be transformed and loaded
-- query time for full refresh of data takes is currently >15 minutes using x-small warehouse, and this will only increase as more data becomes available

with ranked as (
select
    s.date,
    s.player_id,
    s.name,
    row_number() over(partition by date, player_id order by dps desc) as rn,
    s.dps
from {{ ref('stg_skill_details_dps') }} s

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where s.date > (select max(this.date) from {{ this }} as this)
    -- where s.date = dateadd(day,1,(select max(this.date) from {{ this }} as this))
{% endif %}

),

filtered as (
select *
from ranked
where rn = 1
),

named as (
select
    filtered.date,
    filtered.name,
    get(b.value:accounts::array, filtered.player_id)::varchar as account_name,
    get(b.value:names::array, filtered.player_id)::varchar as character_name,
    filtered.dps
from filtered
join {{ source('external','ext_build_dims') }} b
    on filtered.date = b.date
)

select
    date,
    --(left(to_char(date),4)||substr(to_char(date),6,2)||right(to_char(date),2))::integer as date_id,
    {{ dbt_utils.surrogate_key(['character_name', 'account_name']) }} as character_id,
    name as main_dps_skill,
    dps
from named