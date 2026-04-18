with source_data as (

    select distinct
        registration_year_month
    from {{ ref('int_ev_registration_monthly') }}

),

typed_data as (

    select
        to_date(registration_year_month || '-01') as full_date,
        registration_year_month
    from source_data

),

filtered_data as (

    select *
    from typed_data
    where registration_year_month is not null

),

final_model as (

    select
        full_date,
        registration_year_month as year_month
    from filtered_data

)

select *
from final_model
