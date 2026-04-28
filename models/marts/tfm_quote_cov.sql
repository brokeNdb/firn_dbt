{{ config(materialized='table') }}

with cte_loc as (
    select
        io.policy_key,
        io.start_date,
        io.insured_object_key,
        max(coalesce(f.value_string, f.value_number::varchar(200))) as levelofcover
    from {{ source('upstream', 'vw_v_quotes_insured_object') }} as io
    inner join {{ source('upstream', 'int_extra_attribute_value_quote_insured_object') }} as f
        on io.policy_key = f.policy_key
       and io.start_date = f.start_date
       and io.insured_object_key = f.insured_object_key
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on d.attribute_sk = f.attribute_sk
       and d.entity_type = 'quotes_insured_object'
       and d.raw_view_name = 'vw_v_quotes_insured_object_extra_data'
       and d.normalized_name = 'LevelOfCover'
    group by 1, 2, 3
),

principal_coverage_map as (
    select * from {{ ref('int_principal_coverage_mapping') }}
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'DUCKCREEK' as source_system_code,
    q.policy_key,
    q.policy_number,
    q.quotecompleteddate,
    q.brand,
    q.jurisdiction,
    q.product,
    q.productline,
    loc.levelofcover,
    risk.insured_object_desc as risktype,
    cov.coverage_code_key as coveragetype,
    levels.coverage_key,
    levels.coverage_start_date,
    coalesce(pcm.principal_coverage_indicator, 0) as principal_coverage_indicator
from {{ ref('tfm_quote') }} as q
inner join {{ source('upstream', 'vw_v_quotes_xref_policy_levels') }} as levels
    on q.policy_key = levels.policy_key
   and q.quotecompleteddate = levels.policy_start_date
inner join {{ source('upstream', 'vw_v_quotes_insured_object') }} as risk
    on levels.insured_object_key = risk.insured_object_key
   and levels.insured_object_start_date = risk.start_date
   and risk.insured_object_type = 'Risk'
inner join {{ source('upstream', 'vw_v_quotes_coverage') }} as cov
    on levels.coverage_key = cov.coverage_key
   and levels.coverage_start_date = cov.start_date
inner join {{ source('upstream', 'vw_v_quotes_insured_object') }} as riskgroup
    on risk.insured_object_parent_key = riskgroup.insured_object_key
   and risk.start_date = riskgroup.start_date
   and riskgroup.insured_object_type = 'RiskGroup'
inner join cte_loc as loc
    on riskgroup.policy_key = loc.policy_key
   and riskgroup.start_date = loc.start_date
   and riskgroup.insured_object_key = loc.insured_object_key
left join principal_coverage_map as pcm
    on loc.levelofcover = pcm.levelofcover
   and risk.insured_object_desc = pcm.risktype
   and cov.coverage_code_key = pcm.coveragetype
where q.quotecompleteddate is not null
group by
    q.policy_key,
    q.policy_number,
    q.quotecompleteddate,
    q.brand,
    q.jurisdiction,
    q.product,
    q.productline,
    loc.levelofcover,
    risk.insured_object_desc,
    cov.coverage_code_key,
    levels.coverage_key,
    levels.coverage_start_date,
    pcm.principal_coverage_indicator
