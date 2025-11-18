select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    credit_key as unique_field,
    count(*) as n_records

from TMDB_DW.ANALYTICS_analytics.fact_credit
where credit_key is not null
group by credit_key
having count(*) > 1



      
    ) dbt_internal_test