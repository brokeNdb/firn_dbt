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

    select typed_data.*, 'height' as rejection_reason
    from typed_data
    where height is not null
        and height_num is null

    union all

    select typed_data.*, 'number_of_seats' as rejection_reason
    from typed_data
    where number_of_seats is not null
        and number_of_seats_num is null

    union all

    select typed_data.*, 'power_rating' as rejection_reason
    from typed_data
    where power_rating is not null
        and power_rating_num is null

    union all

    select typed_data.*, 'vdam_weight' as rejection_reason
    from typed_data
    where vdam_weight is not null
        and vdam_weight_num is null

    union all

    select typed_data.*, 'vehicle_year' as rejection_reason
    from typed_data
    where vehicle_year is not null
        and vehicle_year_num is null

    union all

    select typed_data.*, 'width' as rejection_reason
    from typed_data
    where width is not null
        and width_num is null

),

final_model as (

    select *
    from filtered_data

)

select *
from final_model
