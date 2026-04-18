with source_data as (

    select
        normalized_location_name,
        population_count
    from {{ ref('stg_statsnz_population') }}

),

typed_data as (

    select
        location_dim.normalized_location_name,
        source_data.population_count
    from source_data
    inner join {{ ref('int_location_dimension_prep') }} as location_dim
        on source_data.normalized_location_name = location_dim.normalized_location_name

),

filtered_data as (

    select *
    from typed_data
    where normalized_location_name is not null

),

final_model as (

    select
        normalized_location_name,
        max(population_count) as population_count
    from filtered_data
    group by 1

)

select *
from final_model
