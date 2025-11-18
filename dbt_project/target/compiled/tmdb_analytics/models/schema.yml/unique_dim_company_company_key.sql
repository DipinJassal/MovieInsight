
    
    

select
    company_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.dim_company
where company_key is not null
group by company_key
having count(*) > 1


