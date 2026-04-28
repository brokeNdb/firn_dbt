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
    qc.levelofcover,
    qc.risktype,
    qc.coveragetype,
    qc.principal_coverage_indicator,
    q.suminsured,
    q.userid,
    q.dimemployeekey,
    q.botquoteflag,
    qc.brand,
    qc.jurisdiction,
    qc.product,
    qc.productline
from {{ ref('int_quote_enriched') }} as q
inner join {{ ref('tfm_quote_cov') }} as qc
    on q.policy_key = qc.policy_key
   and q.policy_number = qc.policy_number
   and q.quotecompleteddate = qc.quotecompleteddate
left join {{ ref('dim_product_coverage') }} as pc
    on pc.producttype = 'Coverage'
   and pc.source_system_code = 'DUCKCREEK'
   and pc.brand = qc.brand
   and pc.jurisdiction = qc.jurisdiction
   and pc.product = qc.product
   and pc.productline = qc.productline
   and pc.levelofcover = qc.levelofcover
   and pc.risktype = qc.risktype
   and pc.coveragetype = qc.coveragetype
