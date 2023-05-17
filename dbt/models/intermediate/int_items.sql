    {{ config(materialized='table', transient=true) }}

with unique_weapon as (
    select
        *,
        'unique_weapon' as source_table
    from {{ ref('stg_unique_weapon') }}
),

unique_armour as (
    select
        *,
        'unique_armour' as source_table
    from {{ ref('stg_unique_armour') }}
),

unique_accessory as (
    select
        *,
        'unique_accessory' as source_table
    from {{ ref('stg_unique_accessory') }}
),

unique_flask as (
    select
        *,
        'unique_flask' as source_table
    from {{ ref('stg_unique_flask') }}
),

unique_jewel as (
    select
        *,
        'unique_jewel' as source_table
    from {{ ref('stg_unique_jewel') }}
),

skill_gem as (
    select
        *,
        'skill_gem' as source_table
    from {{ ref('stg_skill_gem') }}
),

cluster_jewel as (
    select
        *,
        'cluster_jewel' as source_table
    from {{ ref('stg_cluster_jewel') }}
),

economy_unioned as (
    select * from unique_weapon
    union all
    select * from unique_armour
    union all
    select * from unique_accessory
    union all
    select * from unique_flask
    union all
    select * from unique_jewel
    union all
    select * from skill_gem
    union all
    select * from cluster_jewel
)

select
    *
exclude
    value
from economy_unioned