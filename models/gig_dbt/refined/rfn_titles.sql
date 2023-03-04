{{
    config(
        materialized='table_insert'
    )
}}

with max_id as 
(
select
        imdb_id
        ,max(day) as day
from    {{source('imdb_dataset','raw_titles')}}
group
by      imdb_id
)
select  
        t1.imdb_id as imdb_id
        ,title_name
        ,cast(release_dt as date) as release_dt
        ,genre_list
        ,cast(`current_timestamp` as timestamp) as d_extract
        ,current_timestamp as refresh_dt
from    {{source('imdb_dataset','raw_titles')}} t1
inner
join    max_id
on      t1.imdb_id = max_id.imdb_id
and     t1.day = max_id.day