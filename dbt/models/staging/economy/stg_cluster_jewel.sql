{{ config(schema='staging') }}

select
    *
from
    {{ source('external','ext_cluster_jewel') }}