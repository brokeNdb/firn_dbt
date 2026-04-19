-- Purpose: Store charging infrastructure supply as a location-level snapshot fact.
-- Grain: One row per dim_location_key.
-- Transformations: Generate the fact surrogate key and populate the location foreign key with station,
-- connector, and average connector measures for reporting without additional BI joins.

with source_data as (

    select
        location_natural_key,
        station_count,
        connector_count,
        avg_connectors_per_station
    from {{ ref('int_charging_metrics') }}

),

transformed_data as (

    select
        md5('fct_charging_station_snapshot|' || location_natural_key) as fct_charging_station_snapshot_key,
        md5(location_natural_key) as dim_location_key,
        station_count,
        connector_count,
        avg_connectors_per_station
    from source_data

),

final_model as (

    select
        fct_charging_station_snapshot_key,
        dim_location_key,
        station_count,
        connector_count,
        avg_connectors_per_station
    from transformed_data

)

select *
from final_model
