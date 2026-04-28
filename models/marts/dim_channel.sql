{{ config(materialized='table') }}

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'REF' as source_system_code,
    row_number() over (order by channel_code) as dimchannelkey,
    channel_code,
    channel_name,
    channel_desc
from {{ ref('tfm_ref_channel') }}
