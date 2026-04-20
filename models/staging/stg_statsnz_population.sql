-- Purpose: Stage the latest Stats NZ population records at the source location grain.
-- Transformations: Keep the latest population_count, normalize the location name for matching,
-- and filter out rows where population_count is null.


with source_data as (

    select *
    from {{ source('landing', 'statsnz_population') }}

),

typed_data as (

    select
        name as location_name,
        erp21 as population_count,
        trim(lower(name)) as normalized_location_name
    from source_data

),

filtered_data as (

    select *
    from typed_data
    where population_count is not null

),

final_model as (

    select *
    from filtered_data

)

select *
from final_model
