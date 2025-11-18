
    
    

select
    genre_id as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.dim_genre
where genre_id is not null
group by genre_id
having count(*) > 1


