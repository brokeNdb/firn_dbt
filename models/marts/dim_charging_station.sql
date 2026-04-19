with source_data as (

    select
        object_id,
        station_name,
        address,
        operator,
        number_of_connectors,
        latitude,
        longitude,
        geo_point
    from {{ ref('stg_ev_roam_charging_stations') }}

),

transformed_data as (

    select
        md5(object_id) as dim_charging_station_key,
        object_id as station_id,
        station_name,
        address,
        operator as operator_name,
        coalesce(number_of_connectors, 0) as number_of_connectors,
        latitude,
        longitude,
        coalesce(geo_point, st_point(longitude, latitude)) as geo_point
    from source_data
    where object_id is not null

),

final_model as (

    select
        dim_charging_station_key,
        station_id,
        station_name,
        address,
        operator_name,
        number_of_connectors,
        latitude,
        longitude,
        geo_point
    from transformed_data

)

select *
from final_model
