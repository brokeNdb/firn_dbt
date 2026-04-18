with source_data as (

    select
        full_date,
        year_month
    from {{ ref('int_month_spine') }}

),

typed_data as (

    select *
    from source_data

),

filtered_data as (

    select *
    from typed_data

),

final_model as (

    select
        year(full_date) * 100 + month(full_date) as date_key,
        full_date,
        year(full_date) as year,
        month(full_date) as month,
        year_month,
        to_char(full_date, 'MMMM') as month_name
    from filtered_data

)

select *
from final_model
