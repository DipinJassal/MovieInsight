select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select title
from TMDB_DW.ANALYTICS_staging.stg_movies
where title is null



      
    ) dbt_internal_test