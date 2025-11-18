select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select genre_name
from TMDB_DW.ANALYTICS_analytics.dim_genre
where genre_name is null



      
    ) dbt_internal_test