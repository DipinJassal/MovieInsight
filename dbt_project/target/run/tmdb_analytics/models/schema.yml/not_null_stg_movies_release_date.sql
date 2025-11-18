select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select release_date
from TMDB_DW.ANALYTICS_staging.stg_movies
where release_date is null



      
    ) dbt_internal_test