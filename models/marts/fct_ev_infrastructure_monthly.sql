with source_data as (

    select
        ev_registrations.location_key,
        ev_registrations.date_key,
        ev_registrations.vehicle_count,
        ev_registrations.ev_count,
        charging.station_count,
        charging.connector_count,
        population.total_population_count as population_count
    from {{ ref('fct_ev_registration_monthly') }} as ev_registrations
    left join {{ ref('fct_charging_station_snapshot') }} as charging
        on ev_registrations.location_key = charging.location_key
    left join {{ ref('dim_location') }} as location_dim
        on ev_registrations.location_key = location_dim.location_key
    left join (
        select
            normalized_location_name,
            sum(population_count) as total_population_count
        from {{ ref('int_population_by_location') }}
        group by 1
    ) as population
        on location_dim.normalized_location_name = population.normalized_location_name

),

typed_data as (

    select *
    from source_data

),

filtered_data as (

    select *
    from typed_data

),

final_model as (

    select
        location_key,
        date_key,
        vehicle_count,
        ev_count,
        station_count,
        connector_count,
        population_count,
        ev_count * 1.0 / nullif(population_count, 0) * 1000 as ev_per_1000_people,
        station_count * 1.0 / nullif(population_count, 0) * 1000 as chargers_per_1000_people,
        ev_count * 1.0 / nullif(station_count, 0) as ev_to_charger_ratio
    from filtered_data

)

select *
from final_model
