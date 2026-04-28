{{ config(materialized='table') }}

with pol_extra as (
    select
        f.policy_key,
        f.start_date,
        max(case when d.normalized_name = 'Campaign Code' then coalesce(f.value_string, f.value_number::varchar(200)) end) as campaign_code,
        max(case when d.normalized_name = 'TechnologyInteractionChannel' then coalesce(f.value_string, f.value_number::varchar(200)) end) as technologyinteractionchannel,
        max(case when d.normalized_name = 'InceptionTechnologyInteractionChannel' then coalesce(f.value_string, f.value_number::varchar(200)) end) as inceptiontechnologyinteractionchannel,
        max(case when d.normalized_name = 'Brand' then coalesce(f.value_string, f.value_number::varchar(200)) end) as brand,
        max(case when d.normalized_name = 'Product' then coalesce(f.value_string, f.value_number::varchar(200)) end) as product,
        max(case when d.normalized_name = 'ProductLineVersion' then coalesce(f.value_string, f.value_number::varchar(200)) end) as productline,
        max(case when d.normalized_name = 'LastTransactionStatusUser' then coalesce(f.value_string, f.value_number::varchar(200)) end) as lasttransactionstatususer
    from {{ source('upstream', 'int_extra_attribute_value_policy') }} as f
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on d.attribute_sk = f.attribute_sk
       and d.entity_type = 'policy_policy'
       and d.raw_view_name = 'vw_v_policy_policy_extra_data'
    group by 1, 2
),

pol_io_extra as (
    select
        io.policy_key,
        io.start_date,
        max(case when d.normalized_name = 'LevelOfCover' then coalesce(f.value_string, f.value_number::varchar(200)) end) as levelofcover
    from {{ source('upstream', 'vw_v_policy_insured_object') }} as io
    inner join {{ source('upstream', 'int_extra_attribute_value_policy_insured_object') }} as f
        on io.policy_key = f.policy_key
       and io.start_date = f.start_date
       and io.insured_object_key = f.insured_object_key
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on d.attribute_sk = f.attribute_sk
       and d.entity_type = 'policy_insured_object'
       and d.raw_view_name = 'vw_v_policy_insured_object_extra_data'
    where io.insured_object_type in ('Risk', 'RiskGroup', 'Location', 'Asset', 'PromiseDetail')
    group by 1, 2
),

pol as (
    select
        p.policy_key,
        p.policy_number,
        p.policy_from_key,
        p.policy_from_number,
        p.policy_revision_number,
        p.status_key,
        p.policy_state as jurisdiction,
        p.issue_date,
        p.effective_date,
        p.start_date,
        pe.campaign_code,
        pe.technologyinteractionchannel,
        pe.inceptiontechnologyinteractionchannel,
        pe.brand,
        pe.product,
        pe.productline,
        pie.levelofcover,
        pe.lasttransactionstatususer
    from {{ source('upstream', 'vw_v_policy_policy') }} as p
    left join pol_extra as pe
        on p.policy_key = pe.policy_key
       and p.start_date = pe.start_date
    left join pol_io_extra as pie
        on p.policy_key = pie.policy_key
       and p.start_date = pie.start_date
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'DUCKCREEK' as source_system_code,
    policy_key,
    policy_number,
    policy_revision_number,
    policy_from_key,
    policy_from_number,
    status_key,
    issue_date,
    effective_date,
    start_date,
    campaign_code,
    levelofcover,
    lasttransactionstatususer as policy_created_by_userid,
    technologyinteractionchannel,
    inceptiontechnologyinteractionchannel,
    brand,
    jurisdiction,
    product,
    productline
from pol
