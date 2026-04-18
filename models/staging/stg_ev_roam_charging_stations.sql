with source_data as (

    select variant_col
    from {{ source('landing', 'ev_roam_charging_stations') }}

),

typed_data as (

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
        lower(trim(regexp_substr(variant_col:"ADDRESS"::varchar, '[^,]+$', 1, 1))) as normalized_location_name
    from source_data

),

filtered_data as (

    select *
    from typed_data

),

final_model as (

    select
        filtered_data.*,
        st_point(longitude, latitude) as geo_point
    from filtered_data

)

select *
from final_model
