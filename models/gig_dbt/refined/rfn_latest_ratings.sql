{{
    config(
        materialized='table_insert'
    )
}}
with filter_ratings as (
select
        imdb_id
        ,max(d_extract) as max_d
from    {{ref('rfn_ratings')}}
where   cast(d_extract as date) = current_date
group 
by      imdb_id
)
select 
        t1.imdb_id
        ,rating_males_all_ages
        ,rating_males_under_18
        ,rating_males_18_29
        ,rating_males_30_44
        ,rating_males_over_45
        ,rating_females_all_ages
        ,rating_females_under_18
        ,rating_females_18_29
        ,rating_females_30_44
        ,rating_females_over_45
        ,rating_10
        ,rating_9
        ,rating_8
        ,rating_7
        ,rating_6
        ,rating_5
        ,rating_4
        ,rating_3
        ,rating_2
        ,rating_1
        ,((10*rating_10)+(9*rating_9)+(8*rating_8)+(7*rating_7)+(6*rating_6)+(5*rating_5)+(4*rating_4)+(3*rating_3)+(2*rating_2)+(1*rating_1))
        /(rating_10+rating_9+rating_8+rating_7+rating_6+rating_5+rating_4+rating_3+rating_2+rating_1) as rating_average
        ,`rank`
        ,d_extract
        ,current_timestamp as refresh_dt
from    {{ref('rfn_ratings')}} t1
inner
join    filter_ratings
on      filter_ratings.imdb_id=t1.imdb_id
and     filter_ratings.max_d=t1.d_extract