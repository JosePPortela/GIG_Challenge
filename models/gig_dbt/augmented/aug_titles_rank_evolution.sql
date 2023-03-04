{{
    config(
        materialized='table_insert'
    )
}}

with current_rank_aux as 
(
select 
         imdb_id
        ,max(d_extract) as max_extract
from    {{ref('rfn_rank_evolution')}}
where   cast(d_extract as date) = current_date
group
by      imdb_id
),
current_rank as 
(
select
         t1.imdb_id
        ,t1.rank
from    {{ref('rfn_rank_evolution')}} t1
inner
join    current_rank_aux
on      current_rank_aux.imdb_id = t1.imdb_id
and     current_rank_aux.max_extract = t1.d_extract
),
rank_agg as (
select
         imdb_id
        ,max(rank) as max_rank
        ,min(rank) as min_rank
        ,avg(rank) as average_rank
        ,max(case
            when rank=1 then 1
            else 0 
            end) as flg__reached_most_popular 
from    {{ref('rfn_rank_evolution')}}
group
by      imdb_id
)
select
         rank_agg.imdb_id
        ,current_rank.rank as current_rank
        ,max_rank
        ,min_rank
        ,average_rank
        ,current_timestamp as refresh_dt    
from    rank_agg
left
join    current_rank
on      current_rank.imdb_id = rank_agg.imdb_id