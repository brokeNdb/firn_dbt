with source_data as (

    select
        normalized_location_name,
        number_of_connectors
    from {{ ref('stg_ev_roam_charging_stations') }}

),

typed_data as (

    select
        loc.normalized_location_name,
        source_data.number_of_connectors
    from source_data
    inner join {{ ref('int_location_dimension_prep') }} as loc
        on source_data.normalized_location_name = loc.normalized_location_name

),

filtered_data as (

    select *
    from typed_data
    where normalized_location_name is not null
        and normalized_location_name != ''

),

final_model as (

    select
        normalized_location_name,
        count(*) as station_count,
        sum(number_of_connectors) as connector_count,
        avg(number_of_connectors) as avg_connectors_per_station
    from filtered_data
    group by 1

)

select *
from final_model
