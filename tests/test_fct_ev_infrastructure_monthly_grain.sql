select
    location_id,
    month_id,
    count(*) as row_count
from {{ ref('fct_ev_infrastructure_monthly') }}
group by 1, 2
having count(*) > 1
