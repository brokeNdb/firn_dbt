with source_data as (

    select
        normalized_operator_name,
        original_operator_name
    from {{ ref('int_station_operator_prep') }}

),

typed_data as (

    select *
    from source_data

),

filtered_data as (

    select *
    from typed_data
    where normalized_operator_name is not null
        and normalized_operator_name != ''

),

final_model as (

    select
        row_number() over (order by normalized_operator_name) as operator_key,
        normalized_operator_name,
        original_operator_name
    from filtered_data

)

select *
from final_model
