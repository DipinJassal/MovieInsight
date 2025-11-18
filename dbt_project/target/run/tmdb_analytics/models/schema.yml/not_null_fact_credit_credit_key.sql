select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select credit_key
from TMDB_DW.ANALYTICS_analytics.fact_credit
where credit_key is null



      
    ) dbt_internal_test