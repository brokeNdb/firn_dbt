with source_data as (

    select
        station_id,
        geo_point
    from {{ ref('dim_charging_station') }}
    where geo_point is not null

),

transformed_data as (

    select
        md5(source_station.station_id) as int_station_nearest_neighbour_key,
        source_station.station_id,
        neighbour_station.station_id as nearest_station_id,
        st_distance(
            source_station.geo_point::geography,
            neighbour_station.geo_point::geography
        ) / 1000.0 as distance_km
    from source_data as source_station
    inner join source_data as neighbour_station
        on source_station.station_id != neighbour_station.station_id
    qualify row_number() over (
        partition by source_station.station_id
        order by
            st_distance(
                source_station.geo_point::geography,
                neighbour_station.geo_point::geography
            ),
            neighbour_station.station_id
    ) = 1

),

final_model as (

    select
        int_station_nearest_neighbour_key,
        station_id,
        nearest_station_id,
        distance_km
    from transformed_data

)

select *
from final_model
