-- Purpose: Store nearest-neighbour charging station distances at the station grain.
-- Grain: One row per source charging station.
-- Transformations: Generate the fact surrogate key, map both source and nearest stations to dimension keys,
-- and retain the nearest-neighbour distance in kilometers for downstream analysis.

with source_data as (

    select
        station_id,
        nearest_station_id,
        distance_km
    from {{ ref('int_station_nearest_neighbour') }}

),

transformed_data as (

    select
        md5('fct_station_nearest_neighbour|' || station_id) as fct_station_nearest_neighbour_key,
        md5(station_id) as dim_charging_station_key,
        md5(nearest_station_id) as nearest_dim_charging_station_key,
        distance_km
    from source_data

),

final_model as (

    select
        fct_station_nearest_neighbour_key,
        dim_charging_station_key,
        nearest_dim_charging_station_key,
        distance_km
    from transformed_data

)

select *
from final_model
