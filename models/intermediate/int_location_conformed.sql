-- Purpose: Create the single source of truth for conformed territorial authority / city / district geography.
-- Transformations: Normalize location names from vehicle and filtered population sources, keep only the
-- target reporting geography grain, derive location_root, and embed a curated charging alias bridge for downstream joins.

with source_data as (

    select
        lower(trim(tla)) as location_natural_key,
        initcap(lower(trim(tla))) as location_name,
        initcap(lower(trim(tla))) as territorial_authority_name,
        cast(null as varchar) as region_name,
        cast(null as number) as population_count
    from {{ ref('stg_motor_vehicle_register') }}
    where tla is not null
        and trim(tla) <> ''

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

base_locations as (

    select
        location_natural_key,
        location_root,
        any_value(location_name) as location_name,
        any_value(territorial_authority_name) as territorial_authority_name,
        any_value(region_name) as region_name,
        max(population_count) as population_count
    from transformed_data
    group by 1, 2

),

charging_location_aliases as (

    select 'wellington central' as location_match_key, 'wellington city' as location_natural_key
    union all select 'lower hutt', 'lower hutt city'
    union all select 'upper hutt central', 'upper hutt city'
    union all select 'christchurch central city', 'christchurch city'
    union all select 'christchurch', 'christchurch city'
    union all select 'addington', 'christchurch city'
    union all select 'woolston', 'christchurch city'
    union all select 'wigram', 'christchurch city'
    union all select 'hamilton', 'hamilton city'
    union all select 'hamilton east', 'hamilton city'
    union all select 'hamilton new zealand', 'hamilton city'
    union all select 'pukete', 'hamilton city'
    union all select 'rototuna', 'hamilton city'
    union all select 'nelson', 'nelson city'
    union all select 'nelso', 'nelson city'
    union all select 'stoke', 'nelson city'
    union all select 'invercargill', 'invercargill city'
    union all select 'ashburton', 'ashburton district'
    union all select 'masterton', 'masterton district'
    union all select 'paraparaumu', 'kapiti coast district'
    union all select 'waikanae', 'kapiti coast district'
    union all select 'kaiapoi', 'waimakariri district'
    union all select 'rangiora', 'waimakariri district'
    union all select 'oxford', 'waimakariri district'
    union all select 'ashhurst', 'palmerston north city'
    union all select 'fitzherbert', 'palmerston north city'
    union all select 'seaview', 'lower hutt city'
    union all select 'avalon', 'lower hutt city'
    union all select 'moera', 'lower hutt city'
    union all select 'taita', 'lower hutt city'
    union all select 'taitā', 'lower hutt city'
    union all select 'wainuiomata', 'lower hutt city'
    union all select 'kenepuru', 'porirua city'
    union all select 'mana', 'porirua city'
    union all select 'ngaio', 'wellington city'
    union all select 'tawa', 'wellington city'
    union all select 'wellington', 'wellington city'
    union all select 'katikati', 'western bay of plenty district'
    union all select 'west melton', 'selwyn district'
    union all select 'glentunnel', 'selwyn district'
    union all select 'prebbleton', 'selwyn district'
    union all select 'rolleston', 'selwyn district'
    union all select 'hanmer springs', 'hurunui district'
    union all select 'levels', 'timaru district'
    union all select 'wanaka', 'queenstown-lakes district'
    union all select 'queenstown', 'queenstown-lakes district'
    union all select 'blenheim', 'marlborough district'
    union all select 'picton', 'marlborough district'
    union all select 'feilding', 'manawatū district'
    union all select 'balclutha', 'clutha district'
    union all select 'kaitaia', 'far north district'
    union all select 'waihi', 'hauraki district'
    union all select 'murchison', 'tasman district'
    union all select 'orewa', 'auckland'
    union all select 'tirau', 'south waikato district'
    union all select 'golden bay', 'tasman district'
    union all select 'te kauwhata', 'waikato district'
    union all select 'manukau', 'auckland'
    union all select 'roxburgh', 'central otago district'
    union all select 'twizel', 'mackenzie district'
    union all select 'coopers beach', 'far north district'
    union all select 'levin', 'horowhenua district'
    union all select 'whitianga', 'thames-coromandel district'
    union all select 'te haroto', 'taupō district'
    union all select 'bluff', 'invercargill city'
    union all select 'te puke', 'western bay of plenty district'
    union all select 'ravenswood', 'waimakariri district'
    union all select 'waiouru', 'ruapehu district'
    union all select 'cheviot', 'hurunui district'
    union all select 'papatowai', 'clutha district'
    union all select 'owaka', 'clutha district'
    union all select 'waipapa', 'far north district'
    union all select 'paihia', 'far north district'
    union all select 'lincoln', 'selwyn district'
    union all select 'motueka', 'tasman district'
    union all select 'featherston', 'south wairarapa district'
    union all select 'waipukurau', 'central hawke''s bay district'
    union all select 'tekapo', 'mackenzie district'
    union all select 'little river', 'selwyn district'
    union all select 'iwitahi', 'taupō district'
    union all select 'tapanui', 'clutha district'

),

final_model as (

    select
        base_locations.location_natural_key,
        base_locations.location_root,
        base_locations.location_root as location_match_key,
        'base_root' as match_type,
        base_locations.location_name,
        base_locations.territorial_authority_name,
        base_locations.region_name,
        base_locations.population_count
    from base_locations

    union all

    select
        base_locations.location_natural_key,
        base_locations.location_root,
        charging_location_aliases.location_match_key,
        'alias' as match_type,
        base_locations.location_name,
        base_locations.territorial_authority_name,
        base_locations.region_name,
        base_locations.population_count
    from base_locations
    inner join charging_location_aliases
        on base_locations.location_natural_key = charging_location_aliases.location_natural_key

)

select *
from final_model
