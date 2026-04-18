with source_data as (

    select distinct
        month_date,
        registration_year_month
    from {{ ref('int_ev_metrics') }}

),

transformed_data as (

    select
        year(month_date) * 100 + month(month_date) as dim_date_key,
        month_date,
        year(month_date) as year,
        month(month_date) as month,
        to_char(month_date, 'MMMM') as month_name,
        registration_year_month as year_month
    from source_data

),

final_model as (

    select
        dim_date_key,
        month_date,
        year,
        month,
        month_name,
        year_month
    from transformed_data

)

select *
from final_model
