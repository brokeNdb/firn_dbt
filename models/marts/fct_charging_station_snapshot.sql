with source_data as (

    select
        normalized_location_name,
        station_count,
        connector_count,
        avg_connectors_per_station
    from {{ ref('int_charging_station_summary') }}

),

typed_data as (

    select
        location_dim.location_key as location_id,
        source_data.station_count,
        source_data.connector_count,
        source_data.avg_connectors_per_station
    from source_data
    inner join {{ ref('dim_location') }} as location_dim
        on source_data.normalized_location_name = location_dim.normalized_location_name

),

filtered_data as (

    select *
    from typed_data

),

final_model as (

    select
        location_id,
        station_count,
        connector_count,
        avg_connectors_per_station
    from filtered_data

)

select *
from final_model
