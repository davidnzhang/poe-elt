{{ config(materialized='table') }}

with unhashed as (
select
    date,
    name,
    case
        when itemtype in ('Body Armour','Two Handed Sword','Two Handed Axe','Bow','Two Handed Mace','Staff')
            then ifnull(links,0)
        when name = 'Oni-Goroshi' then 6
        else null end as links,
    variant,
    chaosvalue,
    exaltedvalue,
    divinevalue,
    count,
    listingcount
from {{ ref('int_items') }}
where source_table not in ('skill_gem', 'cluster_jewel')
)

select
    {{ generate_date_key(date) }} as date_id,
    {{ dbt_utils.surrogate_key(['name']) }} as item_id,
    links,
    variant,
    chaosvalue,
    exaltedvalue,
    divinevalue,
    count,
    listingcount
from unhashed