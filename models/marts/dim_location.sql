with source_data as (

    select
        location_key,
        normalized_location_name,
        original_location_name
    from {{ ref('int_location_dimension_prep') }}

),

typed_data as (

    select *
    from source_data

),

filtered_data as (

    select *
    from typed_data
    where normalized_location_name is not null

),

final_model as (

    select
        location_key,
        normalized_location_name,
        original_location_name
    from filtered_data

)

select *
from final_model
