-- Purpose: Find the nearest neighbouring charging station for each staged station.
-- Grain: One row per station_id.
-- Transformations: Self-join station coordinates, calculate distance_km with Snowflake HAVERSINE,
-- exclude self-matches, and keep only the nearest neighbour using QUALIFY ROW_NUMBER() = 1.


with source_data as (

    select
        object_id as station_id,
        latitude,
        longitude
    from {{ ref('stg_ev_roam_charging_stations') }}
    where latitude is not null
        and longitude is not null

),

transformed_data as (

    select
        source_station.station_id,
        neighbour_station.station_id as nearest_station_id,
        haversine(
            source_station.latitude,
            source_station.longitude,
            neighbour_station.latitude,
            neighbour_station.longitude
        ) as distance_km
    from source_data as source_station
    inner join source_data as neighbour_station
        on source_station.station_id != neighbour_station.station_id
    qualify row_number() over (
        partition by source_station.station_id
        order by
            haversine(
                source_station.latitude,
                source_station.longitude,
                neighbour_station.latitude,
                neighbour_station.longitude
            ),
            neighbour_station.station_id
    ) = 1

),

final_model as (

    select
        station_id,
        nearest_station_id,
        distance_km
    from transformed_data

)

select *
from final_model
