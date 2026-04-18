with source_data as (

    select
        normalized_location_name,
        number_of_connectors
    from {{ ref('stg_ev_roam_charging_stations') }}
    where normalized_location_name is not null
        and normalized_location_name != ''

),

transformed_data as (

    select
        location_dim.normalized_location_name,
        source_data.number_of_connectors
    from source_data
    inner join {{ ref('int_location_dimension_prep') }} as location_dim
        on source_data.normalized_location_name = location_dim.normalized_location_name

),

aggregated_data as (

    select
        normalized_location_name,
        count(*) as station_count,
        sum(number_of_connectors) as connector_count,
        avg(number_of_connectors) as avg_connectors_per_station
    from transformed_data
    group by 1

),

final_model as (

    select
        normalized_location_name,
        station_count,
        connector_count,
        avg_connectors_per_station
    from aggregated_data

)

select *
from final_model
