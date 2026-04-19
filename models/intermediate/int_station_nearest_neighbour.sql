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
        2 * 6371 * asin(
            sqrt(
                pow(sin(radians(neighbour_station.latitude - source_station.latitude) / 2), 2)
                + cos(radians(source_station.latitude))
                * cos(radians(neighbour_station.latitude))
                * pow(sin(radians(neighbour_station.longitude - source_station.longitude) / 2), 2)
            )
        ) as distance_km
    from source_data as source_station
    inner join source_data as neighbour_station
        on source_station.station_id != neighbour_station.station_id
    qualify row_number() over (
        partition by source_station.station_id
        order by
            2 * 6371 * asin(
                sqrt(
                    pow(sin(radians(neighbour_station.latitude - source_station.latitude) / 2), 2)
                    + cos(radians(source_station.latitude))
                    * cos(radians(neighbour_station.latitude))
                    * pow(sin(radians(neighbour_station.longitude - source_station.longitude) / 2), 2)
                )
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
