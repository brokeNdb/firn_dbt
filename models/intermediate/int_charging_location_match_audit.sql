-- Purpose: Audit charging station location matching into the conformed territorial authority / city / district geography.
-- Transformations: Reuse address-derived city candidates, exact root matching, alias bridging, and fallback
-- normalized location matching to report matched and unmatched charging station coverage.

with source_data as (

    select
        station_city_candidate,
        normalized_location_name,
        address
    from {{ ref('stg_ev_roam_charging_stations') }}
    where station_city_candidate is not null

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
        normalized_location_name,
        address
    from source_data

),

charging_location_aliases as (

    select 'wellington central' as station_city_candidate, 'wellington city' as location_natural_key
    union all select 'lower hutt', 'lower hutt city'
    union all select 'upper hutt central', 'upper hutt city'
    union all select 'christchurch central city', 'christchurch city'
    union all select 'hamilton east', 'hamilton city'
    union all select 'paraparaumu', 'kapiti coast district'
    union all select 'nelso', 'nelson city'
    union all select 'addington', 'christchurch city'
    union all select 'woolston', 'christchurch city'
    union all select 'wigram', 'christchurch city'
    union all select 'kaiapoi', 'waimakariri district'
    union all select 'rangiora', 'waimakariri district'
    union all select 'ashhurst', 'palmerston north city'
    union all select 'fitzherbert', 'palmerston north city'
    union all select 'seaview', 'lower hutt city'
    union all select 'avalon', 'lower hutt city'
    union all select 'kenepuru', 'porirua city'
    union all select 'ngaio', 'wellington city'
    union all select 'tawa', 'wellington city'
    union all select 'mana', 'porirua city'
    union all select 'moera', 'lower hutt city'
    union all select 'taita', 'lower hutt city'
    union all select 'taitā', 'lower hutt city'
    union all select 'stoke', 'nelson city'
    union all select 'katikati', 'western bay of plenty district'
    union all select 'west melton', 'selwyn district'
    union all select 'glentunnel', 'selwyn district'
    union all select 'prebbleton', 'selwyn district'
    union all select 'rolleston', 'selwyn district'
    union all select 'oxford', 'waimakariri district'
    union all select 'hanmer springs', 'hurunui district'
    union all select 'levels', 'timaru district'
    union all select 'hamilton', 'hamilton city'
    union all select 'christchurch', 'christchurch city'
    union all select 'nelson', 'nelson city'
    union all select 'wellington', 'wellington city'
    union all select 'invercargill', 'invercargill city'
    union all select 'ashburton', 'ashburton district'
    union all select 'masterton', 'masterton district'
    union all select 'wanaka', 'queenstown-lakes district'
    union all select 'queenstown', 'queenstown-lakes district'
    union all select 'blenheim', 'marlborough district'
    union all select 'picton', 'marlborough district'
    union all select 'feilding', 'manawatū district'
    union all select 'waikanae', 'kapiti coast district'
    union all select 'balclutha', 'clutha district'
    union all select 'kaitaia', 'far north district'
    union all select 'waihi', 'hauraki district'
    union all select 'murchison', 'tasman district'
    union all select 'orewa', 'auckland'
    union all select 'tirau', 'south waikato district'
    union all select 'golden bay', 'tasman district'
    union all select 'te kauwhata', 'waikato district'
    union all select 'manukau', 'auckland'
    union all select 'pukete', 'hamilton city'
    union all select 'roxburgh', 'central otago district'
    union all select 'twizel', 'mackenzie district'
    union all select 'coopers beach', 'far north district'
    union all select 'levin', 'horowhenua district'
    union all select 'whitianga', 'thames-coromandel district'
    union all select 'wainuiomata', 'lower hutt city'
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
    union all select 'kapiti road', 'kapiti coast district'
    union all select 'tekapo', 'mackenzie district'
    union all select 'little river', 'selwyn district'
    union all select 'iwitahi', 'taupō district'
    union all select 'tapanui', 'clutha district'

),

matched_rows as (

    select
        transformed_data.station_city_candidate,
        transformed_data.normalized_location_name,
        transformed_data.address,
        coalesce(
            exact_root_match.location_natural_key,
            charging_location_aliases.location_natural_key,
            fallback_root_match.location_natural_key
        ) as matched_location_natural_key,
        case
            when exact_root_match.location_natural_key is not null then 'exact_root_match'
            when charging_location_aliases.location_natural_key is not null then 'alias_match'
            when fallback_root_match.location_natural_key is not null then 'fallback_normalized_location_match'
            else 'unmatched'
        end as match_method
    from transformed_data
    left join {{ ref('int_location_conformed') }} as exact_root_match
        on transformed_data.station_city_candidate = exact_root_match.location_root
    left join charging_location_aliases
        on transformed_data.station_city_candidate = charging_location_aliases.station_city_candidate
    left join {{ ref('int_location_conformed') }} as fallback_root_match
        on transformed_data.normalized_location_fallback = fallback_root_match.location_root

),

final_model as (

    select
        'summary' as audit_row_type,
        'source_row_count' as audit_metric,
        count(*)::varchar as audit_value,
        cast(null as varchar) as station_city_candidate,
        cast(null as varchar) as normalized_location_name,
        cast(null as varchar) as address
    from matched_rows

    union all

    select
        'summary' as audit_row_type,
        'exact_root_match_row_count' as audit_metric,
        count_if(match_method = 'exact_root_match')::varchar as audit_value,
        cast(null as varchar) as station_city_candidate,
        cast(null as varchar) as normalized_location_name,
        cast(null as varchar) as address
    from matched_rows

    union all

    select
        'summary' as audit_row_type,
        'alias_match_row_count' as audit_metric,
        count_if(match_method = 'alias_match')::varchar as audit_value,
        cast(null as varchar) as station_city_candidate,
        cast(null as varchar) as normalized_location_name,
        cast(null as varchar) as address
    from matched_rows

    union all

    select
        'summary' as audit_row_type,
        'fallback_match_row_count' as audit_metric,
        count_if(match_method = 'fallback_normalized_location_match')::varchar as audit_value,
        cast(null as varchar) as station_city_candidate,
        cast(null as varchar) as normalized_location_name,
        cast(null as varchar) as address
    from matched_rows

    union all

    select
        'summary' as audit_row_type,
        'unmatched_row_count' as audit_metric,
        count_if(match_method = 'unmatched')::varchar as audit_value,
        cast(null as varchar) as station_city_candidate,
        cast(null as varchar) as normalized_location_name,
        cast(null as varchar) as address
    from matched_rows

    union all

    select
        'unmatched_example' as audit_row_type,
        match_method as audit_metric,
        cast(null as varchar) as audit_value,
        station_city_candidate,
        normalized_location_name,
        address
    from matched_rows
    where match_method = 'unmatched'
    qualify row_number() over (
        partition by station_city_candidate
        order by station_city_candidate, normalized_location_name, address
    ) = 1

)

select *
from final_model
