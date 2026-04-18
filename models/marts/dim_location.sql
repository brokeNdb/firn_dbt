with source_data as (

    select
        location_natural_key,
        location_name,
        territorial_authority_name,
        region_name,
        population_count
    from {{ ref('int_location_conformed') }}

),

transformed_data as (

    select
        md5(location_natural_key) as dim_location_key,
        location_natural_key,
        location_name,
        territorial_authority_name,
        region_name,
        population_count
    from source_data

),

final_model as (

    select
        dim_location_key,
        location_name,
        territorial_authority_name,
        region_name,
        population_count
    from transformed_data

)

select *
from final_model
