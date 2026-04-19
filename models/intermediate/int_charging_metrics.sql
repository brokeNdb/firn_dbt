with source_data as (

    select
        normalized_location_name as location_natural_key,
        operator as operator_name,
        lower(trim(operator)) as operator_natural_key,
        coalesce(number_of_connectors, 0) as number_of_connectors
    from {{ ref('stg_ev_roam_charging_stations') }}
    where normalized_location_name is not null
        and normalized_location_name != ''

),

transformed_data as (

    select
        location_conformed.location_natural_key,
        source_data.operator_name,
        source_data.operator_natural_key,
        source_data.number_of_connectors
    from source_data
    inner join {{ ref('int_location_conformed') }} as location_conformed
        on source_data.location_natural_key = location_conformed.location_natural_key

),

final_model as (

    select
        location_natural_key,
        any_value(operator_name) as operator_name,
        any_value(operator_natural_key) as operator_natural_key,
        count(*) as station_count,
        coalesce(sum(number_of_connectors), 0) as connector_count,
        coalesce(avg(number_of_connectors), 0) as avg_connectors_per_station
    from transformed_data
    group by 1

)

select *
from final_model
