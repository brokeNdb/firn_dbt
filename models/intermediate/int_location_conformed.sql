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
    where normalized_location_name = 'auckland'
        or normalized_location_name like '% city'
        or normalized_location_name like '% district'
        or normalized_location_name like '% territory'

),

transformed_data as (

    select
        location_natural_key,
        location_name,
        territorial_authority_name,
        region_name,
        population_count
    from source_data
    where location_natural_key is not null
        and location_natural_key != ''
        and location_natural_key not like '% region%'
        and location_natural_key not like '% local board area%'
        and location_natural_key != 'new zealand'
        and location_natural_key not like 'north island%'
        and location_natural_key not like 'south island%'

),

final_model as (

    select
        location_natural_key,
        any_value(location_name) as location_name,
        any_value(territorial_authority_name) as territorial_authority_name,
        any_value(region_name) as region_name,
        max(population_count) as population_count
    from transformed_data
    group by 1

)

select *
from final_model
