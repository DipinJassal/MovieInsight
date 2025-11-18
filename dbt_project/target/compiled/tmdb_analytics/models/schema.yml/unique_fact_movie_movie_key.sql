
    
    

select
    movie_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.fact_movie
where movie_key is not null
group by movie_key
having count(*) > 1


