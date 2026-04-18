with source_data as (

    select
        location_dim.location_key,
        location_dim.normalized_location_name,
        location_dim.original_location_name,
        population.population_count
    from {{ ref('int_location_dimension_prep') }} as location_dim
    left join {{ ref('int_population_by_location') }} as population
        on location_dim.normalized_location_name = population.normalized_location_name

),

typed_data as (

    select *
    from source_data

),

filtered_data as (

    select *
    from typed_data

),

final_model as (

    select
        location_key,
        normalized_location_name,
        original_location_name,
        population_count
    from filtered_data

)

select *
from final_model
