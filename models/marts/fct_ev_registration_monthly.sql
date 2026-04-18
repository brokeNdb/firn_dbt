with source_data as (

    select
        normalized_location_name,
        registration_year_month,
        vehicle_count,
        ev_count
    from {{ ref('int_ev_metrics') }}

),

typed_data as (

    select
        location_dim.location_key as location_id,
        date_dim.date_key as month_id,
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
        location_id,
        month_id,
        vehicle_count,
        ev_count
    from filtered_data

)

select *
from final_model
