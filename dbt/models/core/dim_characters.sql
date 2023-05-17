{{ config(materialized='table')}}

select
    character_id,
    character_name,
    account_name
from {{ref('stg_build_dims')}}
group by character_id, character_name, account_name
