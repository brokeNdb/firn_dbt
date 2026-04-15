  select
    *,
    try_to_number(height) as height_num,
    try_to_number(number_of_seats) as number_of_seats_num,
    try_to_number(power_rating) as power_rating_num,
    try_to_number(vdam_weight) as vdam_weight_num,
    try_to_number(vehicle_year) as vehicle_year_num,
    try_to_number(width) as width_num
from FIRN_PROJECT.LANDING.MOTOR_VEHICLE_REGISTER_TABLE