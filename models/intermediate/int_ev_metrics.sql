with source_data as (

    select
        lower(trim(tla)) as normalized_location_name,
        registration_year_month,
        motive_power_group
    from {{ ref('stg_motor_vehicle_register') }}
    where tla is not null
        and registration_year_month is not null

),

transformed_data as (

    select
        location_dim.normalized_location_name,
        source_data.registration_year_month,
        source_data.motive_power_group,
        population.population_count
    from source_data
    inner join {{ ref('int_location_dimension_prep') }} as location_dim
        on source_data.normalized_location_name = location_dim.normalized_location_name
    left join {{ ref('stg_statsnz_population') }} as population
        on location_dim.normalized_location_name = population.normalized_location_name

),

aggregated_data as (

    select
        normalized_location_name,
        registration_year_month,
        count(*) as vehicle_count,
        count(case when motive_power_group = 'EV' then 1 end) as ev_count,
        max(population_count) as population_count
    from transformed_data
    group by 1, 2

),

final_model as (

    select
        normalized_location_name,
        registration_year_month,
        vehicle_count,
        ev_count,
        population_count
    from aggregated_data

)

select *
from final_model
