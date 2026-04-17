-- models/staging/stg_motor_vehicle_register.sql

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

select
    objectid,
    alternative_motive_power,
    basic_colour,
    body_type,
    cc_rating,
    chassis7,
    class,
    engine_number,
    first_nz_registration_year,
    first_nz_registration_month,
    gross_vehicle_mass,
    height,
    import_status,
    industry_class,
    industry_model_code,
    make,
    model,
    motive_power,
    mvma_model_code,
    number_of_axles,
    number_of_seats,
    nz_assembled,
    original_country,
    power_rating,
    previous_country,
    road_transport_code,
    submodel,
    tla,
    transmission_type,
    vdam_weight,
    vehicle_type,
    vehicle_usage,
    vehicle_year,
    vin11,
    width,
    synthetic_greenhouse_gas,
    fc_combined,
    fc_urban,
    fc_extra_urban,

    try_to_number(height) as height_num,
    try_to_number(number_of_seats) as number_of_seats_num,
    try_to_number(power_rating) as power_rating_num,
    try_to_number(vdam_weight) as vdam_weight_num,
    try_to_number(vehicle_year) as vehicle_year_num,
    try_to_number(width) as width_num

from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE
where objectid not in (select objectid from invalid_rows)