with source_data as (

    select
        operator as original_operator_name,
        lower(trim(operator)) as normalized_operator_name
    from {{ ref('stg_ev_roam_charging_stations') }}

),

typed_data as (

    select distinct
        normalized_operator_name,
        original_operator_name
    from source_data

),

filtered_data as (

    select
        any_value(original_operator_name) as original_operator_name,
        normalized_operator_name
    from typed_data
    where normalized_operator_name is not null
        and normalized_operator_name != ''
    group by 2

),

final_model as (

    select
        normalized_operator_name,
        original_operator_name
    from filtered_data

)

select *
from final_model
