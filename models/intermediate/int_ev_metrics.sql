with source_data as (

    select
        lower(trim(tla)) as location_natural_key,
        to_date(registration_year_month || '-01') as month_date,
        registration_year_month,
        motive_power_group
    from {{ ref('stg_motor_vehicle_register') }}
    where tla is not null
        and registration_year_month is not null

),

transformed_data as (

    select
        location_conformed.location_natural_key,
        source_data.month_date,
        source_data.registration_year_month,
        source_data.motive_power_group,
        location_conformed.population_count
    from source_data
    inner join {{ ref('int_location_conformed') }} as location_conformed
        on source_data.location_natural_key = location_conformed.location_natural_key

),

final_model as (

    select
        location_natural_key,
        month_date,
        registration_year_month,
        count(*) as vehicle_count,
        count(case when motive_power_group = 'EV' then 1 end) as ev_count,
        max(population_count) as population_count
    from transformed_data
    group by 1, 2, 3

)

select *
from final_model
