select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select person_id
from TMDB_DW.ANALYTICS_staging.stg_credits
where person_id is null



      
    ) dbt_internal_test