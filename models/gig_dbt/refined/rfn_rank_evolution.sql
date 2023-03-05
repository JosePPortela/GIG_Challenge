{{
    config(
        materialized='table_insert'
    )
}}

with rank_output as (
select  
        *
from    {{ref('rfn_ratings')}}        
)
select
         imdb_id
        ,rank
        ,d_extract
        ,current_timestamp as refresh_dt
from    rank_output                