select
    location_key,
    date_key,
    count(*) as row_count
from {{ ref('fct_ev_infrastructure_monthly') }}
group by 1, 2
having count(*) > 1
