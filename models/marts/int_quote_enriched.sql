{{ config(materialized='view') }}

select
    q.edw_load_timestamp,
    q.edw_mdf_audit_id,
    q.source_system_code,
    q.policy_key,
    q.policy_number,
    q.quote_date,
    d.dim_date_key as quote_date_key,
    q.campaign_code,
    q.quoteratedflag,
    q.quoterateddate,
    coalesce(q.quotecompletedflag, 'NA') as quotecompletedflag,
    q.quotecompleteddate,
    c.dimchannelkey,
    c1.dimchannelkey as quote_start_dimchannelkey,
    q.levelofcover,
    cast(nullif(q.suminsured_raw, '') as decimal(18, 4))::bigint as suminsured,
    q.suminsured_raw,
    q.userid,
    coalesce(u.dim_employee_key, -1) as dimemployeekey,
    case when b.quote_number is not null then 'Y' else 'N' end as botquoteflag,
    q.brand,
    q.jurisdiction,
    q.product,
    q.productline
from {{ ref('tfm_quote') }} as q
left join {{ ref('dim_channel') }} as c
    on q.channel = c.channel_code
left join {{ ref('dim_channel') }} as c1
    on q.channel_start_at = c1.channel_code
left join {{ ref('dim_date') }} as d
    on q.quote_date::date = d.cal_date
left join {{ ref('dim_user') }} as u
    on lower(q.userid) = lower(u.duck_username)
   and q.quote_date between u.start_date and u.end_date
left join {{ ref('tfm_quote_bot_activity') }} as b
    on b.quote_number = q.policy_number
   and b.visitdate = q.quote_date::date
