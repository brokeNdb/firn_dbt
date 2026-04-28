{{ config(materialized='table') }}

with cte_loc as (
    select
        f.policy_key,
        f.start_date,
        f.insured_object_key,
        max(f.value_string) as levelofcover
    from {{ source('upstream', 'int_extra_attribute_value_quote_insured_object') }} as f
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on f.attribute_sk = d.attribute_sk
       and d.entity_type = 'quotes_insured_object'
       and d.raw_view_name = 'vw_v_quotes_insured_object_extra_data'
       and d.normalized_name = 'LevelOfCover'
    group by 1, 2, 3
),

cte_quote_extra as (
    select
        f.policy_key,
        f.start_date,
        max(case when d.normalized_name = 'Product' then f.value_string end) as product,
        max(case when d.normalized_name = 'Brand' then f.value_string end) as brand
    from {{ source('upstream', 'int_extra_attribute_value_quote') }} as f
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on f.attribute_sk = d.attribute_sk
       and d.entity_type = 'quotes'
       and d.raw_view_name = 'vw_v_quotes_extra_data'
       and d.normalized_name in ('Product', 'Brand')
    group by 1, 2
),

principal_coverage_map as (
    select * from {{ ref('int_principal_coverage_mapping') }}
)

select distinct
    qx.brand as brand,
    q.jurisdiction_state as jurisdiction,
    qx.product as product,
    line.line_column_value as productline,
    loc.levelofcover as levelofcover,
    risk.insured_object_desc as risktype,
    cov.coverage_code_key as coveragetype,
    coalesce(pcm.principal_coverage_indicator, 0) as principal_coverage_indicator
from {{ source('upstream', 'vw_v_quotes_all') }} as q
inner join {{ source('upstream', 'vw_v_quotes_xref_policy_levels') }} as levels
    on q.policy_key = levels.policy_key
   and q.start_date = levels.policy_start_date
inner join {{ source('upstream', 'vw_v_quotes_insured_object') }} as risk
    on levels.policy_key = risk.policy_key
   and levels.insured_object_key = risk.insured_object_key
   and levels.insured_object_start_date = risk.start_date
   and risk.insured_object_type = 'Risk'
inner join {{ source('upstream', 'vw_v_quotes_coverage') }} as cov
    on levels.policy_key = cov.policy_key
   and levels.coverage_key = cov.coverage_key
   and levels.coverage_start_date = cov.start_date
inner join {{ source('upstream', 'vw_v_quotes_insured_object') }} as riskgroup
    on risk.policy_key = riskgroup.policy_key
   and risk.insured_object_parent_key = riskgroup.insured_object_key
   and risk.start_date = riskgroup.start_date
   and riskgroup.insured_object_type = 'RiskGroup'
inner join cte_loc as loc
    on riskgroup.policy_key = loc.policy_key
   and riskgroup.start_date = loc.start_date
   and riskgroup.insured_object_key = loc.insured_object_key
inner join cte_quote_extra as qx
    on q.policy_key = qx.policy_key
   and q.start_date = qx.start_date
inner join {{ source('upstream', 'vw_v_quotes_line') }} as line
    on q.policy_key = line.policy_key
   and q.start_date = line.start_date
   and line.line_column_name = 'ProductLine'
left join principal_coverage_map as pcm
    on loc.levelofcover = pcm.levelofcover
   and risk.insured_object_desc = pcm.risktype
   and cov.coverage_code_key = pcm.coveragetype
where q.jurisdiction_state = 'NZ'
