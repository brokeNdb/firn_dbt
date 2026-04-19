-- Purpose: Create the single source of truth for conformed territorial authority / city / district geography.
-- Transformations: Normalize location names from vehicle and filtered population sources, keep only the
-- target reporting geography grain, and derive location_root for reliable downstream matching.

with source_data as (

    select
        lower(trim(tla)) as location_natural_key,
        initcap(lower(trim(tla))) as location_name,
        initcap(lower(trim(tla))) as territorial_authority_name,
        cast(null as varchar) as region_name,
        cast(null as number) as population_count
    from {{ ref('stg_motor_vehicle_register') }}
    where tla is not null

    union all

    select
        normalized_location_name as location_natural_key,
        location_name,
        location_name as territorial_authority_name,
        cast(null as varchar) as region_name,
        population_count
    from {{ ref('stg_statsnz_population') }}
    where (
        normalized_location_name = 'auckland'
        or normalized_location_name like '% city'
        or normalized_location_name like '% district'
        or normalized_location_name like '% territory'
    )
        and normalized_location_name not like '% region%'
        and normalized_location_name not like '% local board area%'
        and normalized_location_name != 'new zealand'
        and normalized_location_name not like 'north island%'
        and normalized_location_name not like 'south island%'

),

transformed_data as (

    select
        location_natural_key,
        trim(regexp_replace(location_natural_key, ' (city|district|territory)$', '')) as location_root,
        location_name,
        territorial_authority_name,
        region_name,
        population_count
    from source_data
    where location_natural_key is not null
        and location_natural_key != ''

),

final_model as (

    select
        location_natural_key,
        location_root,
        any_value(location_name) as location_name,
        any_value(territorial_authority_name) as territorial_authority_name,
        any_value(region_name) as region_name,
        max(population_count) as population_count
    from transformed_data
    group by 1, 2

)

select *
from final_model
