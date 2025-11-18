
    
    

select
    date_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.dim_time
where date_key is not null
group by date_key
having count(*) > 1


