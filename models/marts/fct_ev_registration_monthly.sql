with source_data as (

    select
        location_natural_key,
        month_date,
        vehicle_count,
        ev_count,
        population_count
    from {{ ref('int_ev_metrics') }}

),

transformed_data as (

    select
        md5('fct_ev_registration_monthly|' || location_natural_key || '|' || to_char(month_date, 'YYYY-MM')) as fct_ev_registration_monthly_key,
        md5(location_natural_key) as dim_location_key,
        year(month_date) * 100 + month(month_date) as dim_date_key,
        ev_count,
        vehicle_count,
        population_count
    from source_data

),

final_model as (

    select
        fct_ev_registration_monthly_key,
        dim_location_key,
        dim_date_key,
        ev_count,
        vehicle_count,
        population_count
    from transformed_data

)

select *
from final_model
