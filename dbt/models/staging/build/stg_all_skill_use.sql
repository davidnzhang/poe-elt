{{ config(
    materialized='view',
    schema='staging'
    )
}}

-- Get file date and json object from stage
with stage_1 as (
select
    date,
    value
from {{ source('external','ext_all_skill_use') }}
),

-- Flatten json into key:value pairs - keys are integers and values are lists at this stage i.e. {key: [list elem 1, list elem 2, list elem 3]}
stage_2 as (
select
    stage_1.date,
    flattened_dict.key::integer as key,
    flattened_dict.value
from
    stage_1,
    lateral flatten(input => stage_1.value) flattened_dict
order by key asc),

-- Further flatten values (the list shown as the value in the key:value pairs in stage above)
-- 
stage_3 as (
select
    stage_2.date as date,
    stage_2.key::integer as key,
    flattened_list.index::integer as list_index,
    flattened_list.value::integer as list_elem
from
    stage_2,
    lateral flatten(input => stage_2.value) flattened_list
)

-- Create running total of list elements to get player_id
select
    date,
    key,
    list_index, -- not required
    sum(list_elem) over (partition by date, key order by list_index asc rows between unbounded preceding and current row) as player_id -- np.cumsum
from stage_3
order by date, key, list_index asc

