select
    location_key,
    count(*) as row_count
from {{ ref('fct_charging_station_snapshot') }}
group by 1
having count(*) > 1
