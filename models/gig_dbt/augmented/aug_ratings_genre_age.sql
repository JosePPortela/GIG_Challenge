{{
    config(
        materialized='table_insert'
    )
}}
with f as (
select 
        imdb_id
        ,rating_females_all_ages
        ,rating_males_all_ages
        ,rating_females_under_18
        ,rating_males_under_18
        ,rating_females_18_29
        ,rating_males_18_29
        ,rating_females_30_44
        ,rating_males_30_44
        ,rating_females_over_45
        ,rating_males_over_45
from    {{ref('rfn_latest_ratings')}} 
)
,final as (
SELECT r.* FROM (
  SELECT 
    [
    STRUCT<imdb_id STRING,genre STRING,age STRING, rating FLOAT64>
      (imdb_id,'females','all_ages', MAX(rating_females_all_ages)),
      (imdb_id,'males','all_ages', MAX(rating_males_all_ages)),
      (imdb_id,'females','under_18', MAX(rating_females_under_18)),
      (imdb_id,'males','under_18', MAX(rating_males_under_18)),
      (imdb_id,'females','18_29', MAX(rating_females_18_29)),
      (imdb_id,'males','18_29', MAX(rating_males_18_29)),
      (imdb_id,'females','30_44', MAX(rating_females_30_44)),
      (imdb_id,'males','30_44', MAX(rating_males_30_44)),
      (imdb_id,'females','over_45', MAX(rating_females_over_45)),
      (imdb_id,'males','over_45', MAX(rating_males_over_45))
    ] AS row
FROM    f
group 
by      imdb_id
), UNNEST(row) AS r   
)
select 
        *
        ,current_timestamp as refresh_dt            
from    final