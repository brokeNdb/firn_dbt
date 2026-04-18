with source_data as (

    select
        normalized_location_name,
        registration_year_month,
        vehicle_count,
        ev_count
    from {{ ref('int_ev_registration_monthly') }}

),

typed_data as (

    select
        location_dim.location_key,
        date_dim.date_key,
        source_data.vehicle_count,
        source_data.ev_count
    from source_data
    inner join {{ ref('dim_location') }} as location_dim
        on source_data.normalized_location_name = location_dim.normalized_location_name
    inner join {{ ref('dim_date') }} as date_dim
        on source_data.registration_year_month = date_dim.year_month

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
        ev_count
    from filtered_data

)

select *
from final_model
