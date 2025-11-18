
    
    

select
    credit_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.fact_credit
where credit_key is not null
group by credit_key
having count(*) > 1


