select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select movie_key
from TMDB_DW.ANALYTICS_analytics.fact_movie
where movie_key is null



      
    ) dbt_internal_test