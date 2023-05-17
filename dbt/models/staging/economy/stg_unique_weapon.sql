{{ config(schema='staging') }}

select
    *
from
    {{ source('external','ext_unique_weapon') }}