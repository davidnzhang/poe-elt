{{ config(schema='staging') }}

select
    *
from
    {{ source('external','ext_skill_gem') }}