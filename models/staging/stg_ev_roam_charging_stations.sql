-- Purpose: Clean and type EV charging station records from the landing JSON source.
-- Transformations: Parse JSON attributes, normalize operator and owner fields, cast dates and
-- coordinates, derive boolean flags, create audit-friendly location tokens from address, and build geo_point.

with source_data as (

    select
        variant_col,
        lower(trim(variant_col:"ADDRESS"::varchar)) as address_lc
    from {{ source('landing', 'ev_roam_charging_stations') }}

),

split_parts as (

    select
        source_data.*,
        split(replace(address_lc, ', ', ','), ',') as addr_parts
    from source_data

),

transformed_data as (

    select
        variant_col:"OBJECTID"::varchar as object_id,
        variant_col:"GlobalID"::varchar as global_id,
        variant_col:"NAME"::varchar as station_name,
        variant_col:"ADDRESS"::varchar as address,
        trim(lower(variant_col:"OPERATOR"::varchar)) as operator,
        trim(lower(variant_col:"OWNER"::varchar)) as owner,
        variant_col:"Type"::varchar as type,
        variant_col:"currentType"::varchar as current_type,
        try_to_number(variant_col:"carParkCount"::varchar) as car_park_count,
        try_to_number(variant_col:"numberOfConnectors"::varchar) as number_of_connectors,
        try_to_double(variant_col:"latitude"::varchar) as latitude,
        try_to_double(variant_col:"longitude"::varchar) as longitude,
        try_to_date(variant_col:"dateFirstOperational"::varchar, 'DD/MM/YYYY') as date_first_operational,
        variant_col:"connectorsList"::varchar as connectors_list,
        variant_col:"maxTimeLimit"::varchar as max_time_limit,
        case
            when upper(trim(variant_col:"hasCarparkCost"::varchar)) in ('Y', 'YES', 'TRUE') then true
            when upper(trim(variant_col:"hasCarparkCost"::varchar)) in ('N', 'NO', 'FALSE') then false
            else null
        end as has_carpark_cost,
        case
            when upper(trim(variant_col:"hasChargingCost"::varchar)) in ('Y', 'YES', 'TRUE') then true
            when upper(trim(variant_col:"hasChargingCost"::varchar)) in ('N', 'NO', 'FALSE') then false
            else null
        end as has_charging_cost,
        case
            when upper(trim(variant_col:"hasTouristAttraction"::varchar)) in ('Y', 'YES', 'TRUE') then true
            when upper(trim(variant_col:"hasTouristAttraction"::varchar)) in ('N', 'NO', 'FALSE') then false
            else null
        end as has_tourist_attraction,
        case
            when upper(trim(variant_col:"is24Hours"::varchar)) in ('Y', 'YES', 'TRUE') then true
            when upper(trim(variant_col:"is24Hours"::varchar)) in ('N', 'NO', 'FALSE') then false
            else null
        end as is_24_hours,
        lower(trim(regexp_substr(variant_col:"ADDRESS"::varchar, '[^,]+$', 1, 1))) as normalized_location_name,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        addr_parts[array_size(addr_parts)-1]::string,
                        '\\b(new zealand|nz)\\b',
                        ''
                    ),
                    '\\b\\d{3,4}\\b',
                    ''
                ),
                '\\s+',
                ' '
            )
        ) as last_part_clean,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        iff(array_size(addr_parts) >= 2, addr_parts[array_size(addr_parts)-2]::string, null),
                        '\\b(new zealand|nz)\\b',
                        ''
                    ),
                    '\\b\\d{3,4}\\b',
                    ''
                ),
                '\\s+',
                ' '
            )
        ) as second_last_part_clean
    from split_parts

),

final_model as (

    select
        transformed_data.*,
        case
            when last_part_clean is not null
                and last_part_clean <> ''
                and last_part_clean not in ('new zealand', 'nz')
                then last_part_clean
            else second_last_part_clean
        end as station_city_candidate,
        st_point(longitude, latitude) as geo_point
    from transformed_data

)

select *
from final_model
