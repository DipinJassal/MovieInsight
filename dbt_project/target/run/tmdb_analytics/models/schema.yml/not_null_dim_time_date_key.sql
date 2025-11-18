select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date_key
from TMDB_DW.ANALYTICS_analytics.dim_time
where date_key is null



      
    ) dbt_internal_test