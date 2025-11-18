
    
    

select
    person_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.dim_person
where person_key is not null
group by person_key
having count(*) > 1


