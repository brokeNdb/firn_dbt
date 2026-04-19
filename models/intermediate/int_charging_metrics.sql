with source_data as (

    select
        normalized_location_name,
        operator as operator_name,
        lower(trim(operator)) as operator_natural_key,
        coalesce(number_of_connectors, 0) as number_of_connectors
    from {{ ref('stg_ev_roam_charging_stations') }}
    where normalized_location_name is not null
        and normalized_location_name != ''

),

cleaned_candidates as (

    select
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
        ) as charging_location_candidate,
        operator_name,
        operator_natural_key,
        number_of_connectors
    from source_data

),

alias_mapping as (

    select 'hamilton' as charging_location_candidate, 'hamilton city' as location_natural_key
    union all select 'hamilton east', 'hamilton city'
    union all select 'hamilton 3204', 'hamilton city'
    union all select 'hamilton 3210', 'hamilton city'
    union all select 'hamilton new zealand', 'hamilton city'
    union all select 'pukete', 'hamilton city'
    union all select 'rototuna', 'hamilton city'
    union all select 'christchurch', 'christchurch city'
    union all select 'christchurch central city', 'christchurch city'
    union all select 'addington', 'christchurch city'
    union all select 'cashmere', 'christchurch city'
    union all select 'wigram', 'christchurch city'
    union all select 'woolston', 'christchurch city'
    union all select 'edgeware road & cranford street christchurch', 'christchurch city'
    union all select 'nelson', 'nelson city'
    union all select 'nelsonn', 'nelson city'
    union all select 'stoke', 'nelson city'
    union all select 'lower hutt', 'lower hutt city'
    union all select 'seaview', 'lower hutt city'
    union all select 'avalon', 'lower hutt city'
    union all select 'eastbourne', 'lower hutt city'
    union all select 'taitā', 'lower hutt city'
    union all select 'moera', 'lower hutt city'
    union all select 'wainuiomata', 'lower hutt city'
    union all select 'upper hutt central', 'upper hutt city'
    union all select 'wellington', 'wellington city'
    union all select 'wellington central', 'wellington city'
    union all select 'ngaio', 'wellington city'
    union all select 'khandallah', 'wellington city'
    union all select 'tawa', 'wellington city'
    union all select 'kenepuru', 'porirua city'
    union all select 'porirua', 'porirua city'
    union all select 'invercargill', 'invercargill city'
    union all select 'ashburton', 'ashburton district'
    union all select 'masterton', 'masterton district'
    union all select 'carterton', 'carterton district'
    union all select 'katikati', 'western bay of plenty district'
    union all select 'kaiapoi', 'waimakariri district'
    union all select 'rangiora', 'waimakariri district'
    union all select 'oxford', 'waimakariri district'
    union all select 'hanmer springs', 'hurunui district'
    union all select 'glentunnel', 'selwyn district'
    union all select 'rolleston', 'selwyn district'
    union all select 'prebbleton', 'selwyn district'
    union all select 'west melton', 'selwyn district'
    union all select 'woodend', 'waimakariri district'
    union all select 'methven', 'ashburton district'
    union all select 'timaru', 'timaru district'
    union all select 'taihape', 'rangitikei district'
    union all select 'te kauwhata', 'waikato district'
    union all select 'raglan', 'waikato district'
    union all select 'huntly', 'waikato district'
    union all select 'mercer', 'waikato district'
    union all select 'manukau', 'auckland'
    union all select 'epsom', 'auckland'
    union all select 'mystery creek', 'waipa district'

),

transformed_data as (

    select
        coalesce(alias_mapping.location_natural_key, location_conformed.location_natural_key) as location_natural_key,
        cleaned_candidates.operator_name,
        cleaned_candidates.operator_natural_key,
        cleaned_candidates.number_of_connectors,
        cleaned_candidates.charging_location_candidate
    from cleaned_candidates
    left join alias_mapping
        on cleaned_candidates.charging_location_candidate = alias_mapping.charging_location_candidate
    left join {{ ref('int_location_conformed') }} as location_conformed
        on cleaned_candidates.charging_location_candidate = location_conformed.location_natural_key
    where coalesce(alias_mapping.location_natural_key, location_conformed.location_natural_key) is not null

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
