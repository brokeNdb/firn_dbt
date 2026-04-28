{{ config(materialized='table') }}

with channel as (
    select 'Assisted' as channel_code, 'Assisted' as channel_name, 'Call Center Channel' as channel_desc
    union all
    select 'NZAA' as channel_code, 'NZAA' as channel_name, 'NZAA kiosk/center' as channel_desc
    union all
    select 'Online' as channel_code, 'Online' as channel_name, 'online website channel' as channel_desc
)

select
    channel_code,
    channel_name,
    channel_desc
from channel
