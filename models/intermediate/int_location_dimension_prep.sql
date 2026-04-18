with source_data as (

    select
        'motor_vehicle' as source_system,
        tla as original_location_name,
        lower(trim(tla)) as normalized_location_name
    from {{ ref('stg_motor_vehicle_register') }}
    where tla is not null

    union all

    select
        'population' as source_system,
        location_name as original_location_name,
        normalized_location_name
    from {{ ref('stg_statsnz_population') }}
    where location_name is not null

    union all

    select
        'ev_station' as source_system,
        normalized_location_name as original_location_name,
        normalized_location_name
    from {{ ref('stg_ev_roam_charging_stations') }}
    where normalized_location_name is not null

),

typed_data as (

    select distinct
        source_system,
        original_location_name,
        normalized_location_name
    from source_data

),

filtered_data as (

    select
        min(original_location_name) as original_location_name,
        normalized_location_name
    from typed_data
    where normalized_location_name is not null
        and normalized_location_name != ''
    group by 2

),

final_model as (

    select
        row_number() over (order by normalized_location_name) as location_key,
        normalized_location_name,
        original_location_name
    from filtered_data

)

select *
from final_model
