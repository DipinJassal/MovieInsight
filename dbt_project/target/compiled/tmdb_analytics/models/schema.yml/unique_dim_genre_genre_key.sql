
    
    

select
    genre_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.dim_genre
where genre_key is not null
group by genre_key
having count(*) > 1


