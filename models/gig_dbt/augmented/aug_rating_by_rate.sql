{{
    config(
        materialized='table_insert'
    )
}}
with f as (
select 
        imdb_id
        ,rating_1
        ,rating_2
        ,rating_3
        ,rating_4
        ,rating_5
        ,rating_6
        ,rating_7
        ,rating_8
        ,rating_9
        ,rating_10
        ,d_extract
from    {{ref('rfn_ratings')}} 
)
,final as (
SELECT r.* FROM (
  SELECT 
    [
    STRUCT<imdb_id STRING,rating STRING,d_extract timestamp, number_of_rates INT64>
      (imdb_id,'rating_1',d_extract, MAX(rating_1)),
      (imdb_id,'rating_2',d_extract, MAX(rating_2)),
      (imdb_id,'rating_3',d_extract, MAX(rating_3)),
      (imdb_id,'rating_4',d_extract, MAX(rating_4)),
      (imdb_id,'rating_5',d_extract, MAX(rating_5)),
      (imdb_id,'rating_6',d_extract, MAX(rating_6)),
      (imdb_id,'rating_7',d_extract, MAX(rating_7)),
      (imdb_id,'rating_8',d_extract, MAX(rating_8)),
      (imdb_id,'rating_9',d_extract, MAX(rating_9)),
      (imdb_id,'rating_10',d_extract, MAX(rating_10))
    ] AS row
FROM    f
group 
by      imdb_id
        ,d_extract
), UNNEST(row) AS r   
)
select 
        *
        ,current_timestamp as refresh_dt            
from    final