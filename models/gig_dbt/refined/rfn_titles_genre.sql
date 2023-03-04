{{
    config(
        materialized='table_insert'
    )
}}

with t1 as 
(
select imdb_id,split(genre_list,',') as split_genre
from {{ref('rfn_titles')}}
)
select 
        imdb_id
        ,trim(split_genre) as genre
        ,current_timestamp as refresh_dt        
from t1
CROSS JOIN UNNEST(t1.split_genre) AS split_genre
