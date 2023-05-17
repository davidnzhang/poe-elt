{{ config(
    materialized='view',
    schema='staging'
    )
}}

-- NOTE: not used

select
    skilldetails.date,
    skilldetails.name,
    f.key::integer as support_gem_key,
    sum(f1.value::integer) over (partition by date, name, support_gem_key order by f1.index::integer asc rows between unbounded preceding and current row) as player_id
from {{ source('external','ext_skill_details')}} skilldetails,
    lateral flatten(input => skilldetails.supportGems::variant, path => 'use') f,
    lateral flatten(input => f.value) f1