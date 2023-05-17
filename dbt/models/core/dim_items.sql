{{ config(materialized='table') }}

select 
    {{ dbt_utils.surrogate_key(['name']) }} as item_id,
    name as item_name,
    max(levelrequired) as level_required,
    max(basetype) as base_type,
    case 
        when source_table = 'unique_flask' then 'Flask'
        when source_table = 'unique_jewel' then 'Jewel'
        when source_table = 'unique_weapon' then 'Weapon'
        else itemtype end as slot,
    max(flavourtext) as flavour_text
from {{ ref('int_items') }}
where source_table not in ('skill_gem', 'cluster_jewel') -- to be added after review at a later stage
group by item_name, slot
order by item_name