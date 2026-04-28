{{ config(materialized='table') }}

-- TODO: new_policy_dim_coverage_key does not match.
with policy as (
    select
        policy_key,
        policy_number,
        policy_revision_number,
        min(start_date) as first_transaction_date,
        max(case when status_key = 'New' and policy_from_key like 'Q%' then policy_from_key end) as policy_quote_key,
        max(case when status_key = 'New' and policy_from_key like 'Q%' then policy_from_number end) as policy_from_number,
        max(case when status_key = 'New' then issue_date end) as policy_issue_date,
        max(case when status_key = 'New' then effective_date end) as policy_effective_date,
        max(case when status_key = 'New' then technologyinteractionchannel end) as raw_policy_created_by_channel,
        max(case when status_key = 'New' then policy_created_by_userid end) as policy_created_by_userid,
        max(case when status_key = 'New' then levelofcover end) as initial_levelofcover,
        max(case when status_key = 'New' then brand end) as brand,
        max(case when status_key = 'New' then jurisdiction end) as jurisdiction,
        max(case when status_key = 'New' then product end) as product,
        max(case when status_key = 'New' then productline end) as productline,
        max(campaign_code) as campaign_code
    from {{ ref('tfm_policy') }}
    group by 1, 2, 3
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'DUCKCREEK' as source_system_code,
    policy.policy_key,
    policy.policy_number,
    policy.policy_revision_number,
    policy.policy_quote_key,
    policy.policy_from_number,
    policy.campaign_code,
    case when policy.policy_from_number like 'QUT%' then 'Y' else 'N' end as new_sale_flag,
    d.dim_date_key as first_transaction_date_key,
    policy.policy_issue_date as new_policy_issue_date,
    policy.policy_effective_date as new_policy_effective_date,
    c.dimchannelkey as new_policy_dim_channel_key,
    pc.dim_coverage_key as new_policy_dim_coverage_key,
    policy.policy_created_by_userid,
    coalesce(u.dim_employee_key, -1) as dimemployeekey
from policy
left join {{ ref('int_channel_mapping') }} as ch
    on policy.raw_policy_created_by_channel = ch.raw_interaction_value
left join {{ ref('dim_channel') }} as c
    on ch.normalized_channel = c.channel_code
left join {{ ref('dim_product_coverage') }} as pc
    on pc.producttype = 'RiskGroup'
   and pc.source_system_code = 'DUCKCREEK'
   and pc.brand = policy.brand
   and pc.jurisdiction = policy.jurisdiction
   and pc.product = policy.product
   and pc.productline = policy.productline
   and pc.levelofcover = policy.initial_levelofcover
left join {{ ref('dim_date') }} as d
    on policy.first_transaction_date::date = d.cal_date
left join {{ ref('dim_user') }} as u
    on lower(trim(policy.policy_created_by_userid)) = lower(u.duck_username)
   and policy.first_transaction_date::date between u.start_date and u.end_date
