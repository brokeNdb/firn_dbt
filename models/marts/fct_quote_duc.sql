{{ config(materialized='table') }}

select
    q.edw_load_timestamp,
    q.edw_mdf_audit_id,
    q.source_system_code,
    q.policy_key,
    q.policy_number,
    q.quote_date_key,
    q.quote_date,
    q.campaign_code,
    q.quoteratedflag,
    q.quoterateddate,
    q.quotecompletedflag,
    q.quotecompleteddate,
    q.dimchannelkey,
    q.quote_start_dimchannelkey,
    coalesce(pc.dim_coverage_key, '-1') as dim_product_coverage_key,
    q.suminsured,
    q.userid,
    q.dimemployeekey,
    q.botquoteflag
from {{ ref('int_quote_enriched') }} as q
left join {{ ref('dim_product_coverage') }} as pc
    on pc.producttype = 'RiskGroup'
   and pc.source_system_code = 'DUCKCREEK'
   and pc.brand = q.brand
   and pc.jurisdiction = q.jurisdiction
   and pc.product = q.product
   and pc.productline = q.productline
   and pc.levelofcover = q.levelofcover
