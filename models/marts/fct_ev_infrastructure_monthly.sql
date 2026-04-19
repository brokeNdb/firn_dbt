with source_data as (

    select
        ev_metrics.location_natural_key,
        ev_metrics.month_date,
        ev_metrics.ev_count,
        coalesce(charging_metrics.station_count, 0) as station_count,
        coalesce(charging_metrics.connector_count, 0) as connector_count,
        ev_metrics.population_count
    from {{ ref('int_ev_metrics') }} as ev_metrics
    left join {{ ref('int_charging_metrics') }} as charging_metrics
        on ev_metrics.location_natural_key = charging_metrics.location_natural_key

),

transformed_data as (

    select
        md5('fct_ev_infrastructure_monthly|' || location_natural_key || '|' || to_char(month_date, 'YYYY-MM')) as fct_ev_infrastructure_monthly_key,
        md5(location_natural_key) as dim_location_key,
        year(month_date) * 100 + month(month_date) as dim_date_key,
        ev_count,
        station_count,
        connector_count,
        population_count,
        ev_count * 1.0 / nullif(station_count, 0) as evs_per_station,
        ev_count * 1.0 / nullif(connector_count, 0) as evs_per_connector,
        population_count * 1.0 / nullif(station_count, 0) as population_per_station,
        (
            ev_count - lag(ev_count) over (
                partition by location_natural_key
                order by month_date
            )
        ) * 1.0 / nullif(
            lag(ev_count) over (
                partition by location_natural_key
                order by month_date
            ),
            0
        ) as ev_growth_rate,
        (
            station_count - lag(station_count) over (
                partition by location_natural_key
                order by month_date
            )
        ) * 1.0 / nullif(
            lag(station_count) over (
                partition by location_natural_key
                order by month_date
            ),
            0
        ) as station_growth_rate,
        case
            when ev_count * 1.0 / nullif(station_count, 0) > 50 then 'Critical'
            when ev_count * 1.0 / nullif(station_count, 0) > 20 then 'Moderate'
            else 'Well Supplied'
        end as infrastructure_gap_flag
    from source_data

),

final_model as (

    select
        fct_ev_infrastructure_monthly_key,
        dim_location_key,
        dim_date_key,
        ev_count,
        station_count,
        connector_count,
        population_count,
        evs_per_station,
        evs_per_connector,
        population_per_station,
        ev_growth_rate,
        station_growth_rate,
        infrastructure_gap_flag
    from transformed_data

)

select *
from final_model
