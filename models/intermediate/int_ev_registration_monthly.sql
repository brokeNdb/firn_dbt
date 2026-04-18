with source_data as (

    select
        lower(trim(tla)) as normalized_location_name,
        registration_year_month,
        motive_power_group
    from {{ ref('stg_motor_vehicle_register') }}

),

typed_data as (

    select
        location_dim.normalized_location_name,
        source_data.registration_year_month,
        source_data.motive_power_group
    from source_data
    inner join {{ ref('int_location_dimension_prep') }} as location_dim
        on source_data.normalized_location_name = location_dim.normalized_location_name

),

filtered_data as (

    select *
    from typed_data
    where registration_year_month is not null

),

final_model as (

    select
        normalized_location_name,
        registration_year_month,
        count(*) as vehicle_count,
        count(case when motive_power_group = 'EV' then 1 end) as ev_count
    from filtered_data
    group by 1, 2

)

select *
from final_model
