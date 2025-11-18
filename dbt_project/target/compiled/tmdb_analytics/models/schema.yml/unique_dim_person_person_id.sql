
    
    

select
    person_id as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.dim_person
where person_id is not null
group by person_id
having count(*) > 1


