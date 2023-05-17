{{ config(materialized='incremental') }}

with stage_1 as(
select
    u.date,
    i.name as item_name,
    d.character_id
from {{ ref('stg_unique_item_use') }} u
inner join {{ ref('stg_build_dims') }} d
    on u.player_id = d.player_id
    and u.date = d.date
inner join {{ ref('stg_unique_items') }} i
    on u.key = i.index
    and u.date = i.date

{% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where (left(to_char(u.date),4)||substr(to_char(u.date),6,2)||right(to_char(u.date),2))::integer > (select max(this.date_id) from {{ this }} as this)

{% endif %}

order by u.date, d.character_id

),

stage_2 as (
select
    s.date,
    d.item_id,
    s.character_id
from stage_1 s
inner join {{ ref('dim_items') }} d
    on s.item_name = d.item_name
)

select
    {{ generate_date_key(date) }} as date_id,
    item_id,
    character_id
from stage_2

