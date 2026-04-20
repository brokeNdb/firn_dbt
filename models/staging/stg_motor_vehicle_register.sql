-- Purpose: Clean and type motor vehicle registration records from the landing source.
-- Transformations: Cast numeric-like fields, filter out rows with failed numeric parsing,
-- and derive motive_power_group plus registration_year_month for downstream EV analytics.


with source_data as (

    select *
    from {{ source('landing', 'motor_vehicle_register') }}

),

typed_data as (

    select
        source_data.*,
        try_to_number(height::varchar) as height_num,
        try_to_number(number_of_seats::varchar) as number_of_seats_num,
        try_to_number(power_rating::varchar) as power_rating_num,
        try_to_number(vdam_weight::varchar) as vdam_weight_num,
        try_to_number(vehicle_year::varchar) as vehicle_year_num,
        try_to_number(width::varchar) as width_num
    from source_data

),

filtered_data as (

    select *
    from typed_data
    where not (
        (height is not null and height_num is null)
        or (number_of_seats is not null and number_of_seats_num is null)
        or (power_rating is not null and power_rating_num is null)
        or (vdam_weight is not null and vdam_weight_num is null)
        or (vehicle_year is not null and vehicle_year_num is null)
        or (width is not null and width_num is null)
    )

),

final_model as (

    select
        filtered_data.*,
        case
            when lower(coalesce(motive_power, '')) like '%electric%' then 'EV'
            when lower(coalesce(motive_power, '')) like '%hybrid%' then 'Hybrid'
            else 'Other'
        end as motive_power_group,
        case
            when try_to_number(first_nz_registration_year::varchar) is not null
                and try_to_number(first_nz_registration_month::varchar) between 1 and 12
                then concat(
                    lpad(to_varchar(try_to_number(first_nz_registration_year::varchar)), 4, '0'),
                    '-',
                    lpad(to_varchar(try_to_number(first_nz_registration_month::varchar)), 2, '0')
                )
            else null
        end as registration_year_month
    from filtered_data

)

select *
from final_model
