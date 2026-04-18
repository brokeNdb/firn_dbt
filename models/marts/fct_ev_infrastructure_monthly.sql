with source_data as (

    select
        ev_registrations.location_id,
        ev_registrations.month_id,
        ev_registrations.ev_count,
        charging.station_count,
        charging.connector_count,
        location_dim.population_count
    from {{ ref('fct_ev_registration_monthly') }} as ev_registrations
    left join {{ ref('fct_charging_station_snapshot') }} as charging
        on ev_registrations.location_id = charging.location_id
    inner join {{ ref('dim_location') }} as location_dim
        on ev_registrations.location_id = location_dim.location_key
    inner join {{ ref('dim_date') }} as date_dim
        on ev_registrations.month_id = date_dim.date_key

),

joined_data as (

    select
        location_id,
        month_id,
        ev_count,
        station_count,
        connector_count as connectors_count,
        population_count
    from source_data

),

calculated_metrics as (

    select
        location_id,
        month_id,
        ev_count,
        station_count,
        connectors_count,
        population_count,
        ev_count * 1.0 / nullif(station_count, 0) as evs_per_station,
        ev_count * 1.0 / nullif(connectors_count, 0) as evs_per_connector,
        population_count * 1.0 / nullif(station_count, 0) as population_per_station,
        (
            ev_count - lag(ev_count) over (
                partition by location_id
                order by month_id
            )
        ) * 1.0 / nullif(
            lag(ev_count) over (
                partition by location_id
                order by month_id
            ),
            0
        ) as ev_growth_rate,
        (
            station_count - lag(station_count) over (
                partition by location_id
                order by month_id
            )
        ) * 1.0 / nullif(
            lag(station_count) over (
                partition by location_id
                order by month_id
            ),
            0
        ) as station_growth_rate,
        case
            when ev_count * 1.0 / nullif(station_count, 0) > 50 then 'Under-supplied'
            when ev_count * 1.0 / nullif(station_count, 0) between 20 and 50 then 'Balanced'
            else 'Well-supplied'
        end as infrastructure_gap_flag
    from joined_data

),

final_model as (

    select
        location_id,
        month_id,
        ev_count,
        station_count,
        connectors_count,
        population_count,
        evs_per_station,
        evs_per_connector,
        population_per_station,
        ev_growth_rate,
        station_growth_rate,
        infrastructure_gap_flag
    from calculated_metrics

)

select *
from final_model
