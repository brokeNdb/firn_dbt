-- Purpose: Summarize charging infrastructure by conformed territorial authority / city / district geography.
-- Grain: One row per location_natural_key.
-- Transformations: Derive a city candidate from address, map it through the shared location conformance map,
-- and aggregate station and connector supply metrics.

with source_data as (

    select
        station_city_candidate,
        normalized_location_name,
        operator as operator_name,
        lower(trim(operator)) as operator_natural_key,
        coalesce(number_of_connectors, 0) as number_of_connectors,
        address
    from {{ ref('stg_ev_roam_charging_stations') }}
    where station_city_candidate is not null
        and station_city_candidate != ''

),

transformed_data as (

    select
        station_city_candidate,
        trim(
            regexp_replace(
                replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(normalized_location_name, '\\b[0-9]{3,4}\\b', ' '),
                            '\\bnew zealand\\b', ' '
                        ),
                        '\\bnz\\b', ' '
                    ),
                    'nelso',
                    'nelson'
                ),
                '\\s+',
                ' '
            )
        ) as normalized_location_fallback,
        operator_name,
        operator_natural_key,
        number_of_connectors,
        address,
        normalized_location_name
    from source_data

),

final_model as (

    select
        matched_locations.location_natural_key,
        any_value(transformed_data.operator_name) as operator_name,
        any_value(transformed_data.operator_natural_key) as operator_natural_key,
        count(*) as station_count,
        coalesce(sum(transformed_data.number_of_connectors), 0) as connector_count,
        coalesce(avg(transformed_data.number_of_connectors), 0) as avg_connectors_per_station
    from transformed_data
    inner join {{ ref('int_location_conformed') }} as matched_locations
        on transformed_data.station_city_candidate = matched_locations.location_match_key
        or transformed_data.normalized_location_fallback = matched_locations.location_match_key
    group by 1

)

select *
from final_model
