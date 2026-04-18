with source_data as (

    select
        min(to_date(registration_year_month || '-01')) as min_month,
        max(to_date(registration_year_month || '-01')) as max_month
    from {{ ref('int_ev_registration_monthly') }}

),

typed_data as (

    select
        min_month,
        max_month,
        datediff(month, min_month, max_month) + 1 as month_count
    from source_data

),

filtered_data as (

    select
        dateadd(month, generated_month.value::integer, min_month) as full_date
    from typed_data,
        lateral flatten(input => array_generate_range(0, month_count)) as generated_month

),

final_model as (

    select
        year(full_date) * 100 + month(full_date) as date_key,
        full_date,
        year(full_date) as year,
        month(full_date) as month,
        to_char(full_date, 'YYYY-MM') as year_month,
        to_char(full_date, 'MMMM') as month_name
    from filtered_data

)

select *
from final_model
