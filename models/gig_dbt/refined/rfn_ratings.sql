{{
    config(
        materialized='table_insert'
    )
}}

with max_id as 
(
select
        imdb_id
from    {{source('imdb_dataset','raw_ratings')}}
where   day>=20230302
)
select  
        t1.imdb_id
        ,title_name
        ,cast(rating_males_all_ages as FLOAT64) as rating_males_all_ages
        ,cast(rating_males_18_29 as FLOAT64) as rating_males_18_29
        ,cast(rating_males_30_44 as FLOAT64) as rating_males_30_44
        ,cast(rating_males_over_45 as FLOAT64) as rating_males_over_45
        ,cast(rating_females_all_ages as FLOAT64) as rating_females_all_ages
        ,cast(rating_females_under_18 as FLOAT64) as rating_females_under_18
        ,cast(rating_females_18_29 as FLOAT64) as rating_females_18_29
        ,cast(rating_females_30_44 as FLOAT64) as rating_females_30_44
        ,cast(rating_females_over_45  as FLOAT64) as  rating_females_over_45  
        ,cast(rating_10 as INT64) as rating_10
        ,cast(rating_9 as INT64) as rating_9
        ,cast(rating_8 as INT64) as rating_8
        ,cast(rating_7 as INT64) as rating_7
        ,cast(rating_6 as INT64) as rating_6
        ,cast(rating_5 as INT64) as rating_5
        ,cast(rating_4 as INT64) as rating_4
        ,cast(rating_3 as INT64) as rating_3
        ,cast(rating_2 as INT64) as rating_2
        ,cast(rating_1 as INT64) as rating_1
        ,cast(`rank` as INT64) as `rank`
        ,cast(`current_timestamp` as timestamp) as d_extract
        ,current_timestamp as refresh_dt
from    {{source('imdb_dataset','raw_ratings')}} t1
inner
join    max_id
on      t1.imdb_id = max_id.imdb_id