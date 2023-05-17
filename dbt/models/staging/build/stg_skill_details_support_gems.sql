{{ config(
    materialized='view',
    schema='staging'
    )
}}

-- NOTE: not used

select
    skilldetails.date,
    skilldetails.name,
    f.key as support_gem,
    f.value as support_gem_key
from {{ source('external','ext_skill_details')}} skilldetails,
    lateral flatten(input => skilldetails.supportGems::variant, path => 'dictionary') f