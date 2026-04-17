-- models/staging/stg_motor_vehicle_register_rejected.sql

with invalid_rows as (

    select objectid
    from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
    where try_to_number(height) is null and height is not null

    union

    select objectid
    from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
    where try_to_number(number_of_seats) is null and number_of_seats is not null

    union

    select objectid
    from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
    where try_to_number(power_rating) is null and power_rating is not null

    union

    select objectid
    from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
    where try_to_number(vdam_weight) is null and vdam_weight is not null

    union

    select objectid
    from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
    where try_to_number(vehicle_year) is null and vehicle_year is not null

    union

    select objectid
    from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
    where try_to_number(width) is null and width is not null

)

select *
from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
where objectid in (select objectid from invalid_rows)