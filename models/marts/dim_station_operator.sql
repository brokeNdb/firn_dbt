with source_data as (

    select distinct
        operator_natural_key,
        operator_name
    from {{ ref('int_charging_metrics') }}
    where operator_natural_key is not null
        and operator_natural_key != ''

),

transformed_data as (

    select
        md5(operator_natural_key) as dim_station_operator_key,
        operator_natural_key,
        operator_name
    from source_data

),

final_model as (

    select
        dim_station_operator_key,
        operator_name
    from transformed_data

)

select *
from final_model
