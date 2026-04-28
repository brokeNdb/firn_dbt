{{ config(materialized='table') }}

-- Purpose:
-- - Build dim_product_coverage at Brand / Jurisdiction / Product / ProductLine / LevelOfCover / RiskType / CoverageType grain.
-- - Source validated RiskType / CoverageType combinations from tfm_product_coverage.
-- - Keep Duck Creek hierarchy to RiskGroup -> Risk -> Coverage.
-- - Carry principal_coverage_indicator on Coverage rows only.

with base_source as (
    select distinct
        src.brand,
        src.jurisdiction,
        src.product,
        src.productline,
        src.levelofcover,
        src.risktype,
        src.coveragetype,
        src.principal_coverage_indicator,
        case
            when src.risktype = 'Residential Property'
                and src.coveragetype = 'Defined Events'
                and src.levelofcover in ('Building', 'Building and Contents', 'Building and Limited Contents', 'Landlord Building', 'Landlord Building and Contents')
                then 'Building'
            when src.risktype = 'Property Contents'
                and src.coveragetype = 'Defined Events'
                and src.levelofcover in ('Building and Contents', 'Building and Limited Contents', 'Contents', 'Landlord Building and Contents', 'Limited Contents')
                then 'Contents'
            when src.risktype = 'Motorcar'
                and src.coveragetype = 'Own Property Damage'
                and src.levelofcover = 'Comprehensive'
                then 'Motor Comprehensive'
            when src.risktype = 'Motorcar'
                and src.coveragetype = 'Fire and Theft'
                and src.levelofcover = 'Third Party Fire and Theft'
                then 'Motor Third Party Fire and Theft'
            when src.risktype = 'Vehicle Liability'
                and src.coveragetype = 'Third Party Property Damage'
                and src.levelofcover = 'Third Party Property Damage'
                then 'Motor Third Party Property Damage'
            else null
        end as aai_product
    from {{ ref('tfm_product_coverage') }} as src
),

cte_base as (
    select distinct
        brand,
        jurisdiction,
        product,
        productline,
        levelofcover,
        risktype,
        coveragetype,
        aai_product,
        principal_coverage_indicator
    from base_source
),

cte_riskgroup as (
    select distinct
        sha2(
            upper(
                coalesce(trim(brand), '~*null*~')
                || '^' || coalesce(trim(jurisdiction), '~*null*~')
                || '^' || coalesce(trim(product), '~*null*~')
                || '^' || coalesce(trim(productline), '~*null*~')
                || '^' || coalesce(trim(levelofcover), '~*null*~')
            ),
            256
        ) as dim_coverage_key,
        sha2(
            upper(
                coalesce(trim(brand), '~*null*~')
                || '^' || coalesce(trim(jurisdiction), '~*null*~')
                || '^' || coalesce(trim(product), '~*null*~')
                || '^' || coalesce(trim(productline), '~*null*~')
                || '^' || coalesce(trim(levelofcover), '~*null*~')
            ),
            256
        ) as dim_coverage_key_parent,
        brand,
        jurisdiction,
        product,
        productline,
        levelofcover,
        null::varchar(100) as risktype,
        null::varchar(100) as coveragetype,
        'RiskGroup'::varchar(20) as producttype,
        null::varchar(100) as aai_product,
        '1'::varchar(10) as hierarchy_level,
        'DUCKCREEK'::varchar(20) as source_system_code,
        null::int as principal_coverage_indicator
    from cte_base
),

cte_risk as (
    select distinct
        sha2(
            upper(
                coalesce(trim(brand), '~*null*~')
                || '^' || coalesce(trim(jurisdiction), '~*null*~')
                || '^' || coalesce(trim(product), '~*null*~')
                || '^' || coalesce(trim(productline), '~*null*~')
                || '^' || coalesce(trim(levelofcover), '~*null*~')
                || '^' || coalesce(trim(risktype), '~*null*~')
            ),
            256
        ) as dim_coverage_key,
        sha2(
            upper(
                coalesce(trim(brand), '~*null*~')
                || '^' || coalesce(trim(jurisdiction), '~*null*~')
                || '^' || coalesce(trim(product), '~*null*~')
                || '^' || coalesce(trim(productline), '~*null*~')
                || '^' || coalesce(trim(levelofcover), '~*null*~')
            ),
            256
        ) as dim_coverage_key_parent,
        brand,
        jurisdiction,
        product,
        productline,
        levelofcover,
        risktype,
        null::varchar(100) as coveragetype,
        'Risk'::varchar(20) as producttype,
        null::varchar(100) as aai_product,
        '2'::varchar(10) as hierarchy_level,
        'DUCKCREEK'::varchar(20) as source_system_code,
        null::int as principal_coverage_indicator
    from cte_base
),

cte_coverage as (
    select distinct
        sha2(
            upper(
                coalesce(trim(brand), '~*null*~')
                || '^' || coalesce(trim(jurisdiction), '~*null*~')
                || '^' || coalesce(trim(product), '~*null*~')
                || '^' || coalesce(trim(productline), '~*null*~')
                || '^' || coalesce(trim(levelofcover), '~*null*~')
                || '^' || coalesce(trim(risktype), '~*null*~')
                || '^' || coalesce(trim(coveragetype), '~*null*~')
            ),
            256
        ) as dim_coverage_key,
        sha2(
            upper(
                coalesce(trim(brand), '~*null*~')
                || '^' || coalesce(trim(jurisdiction), '~*null*~')
                || '^' || coalesce(trim(product), '~*null*~')
                || '^' || coalesce(trim(productline), '~*null*~')
                || '^' || coalesce(trim(levelofcover), '~*null*~')
                || '^' || coalesce(trim(risktype), '~*null*~')
            ),
            256
        ) as dim_coverage_key_parent,
        brand,
        jurisdiction,
        product,
        productline,
        levelofcover,
        risktype,
        coveragetype,
        'Coverage'::varchar(20) as producttype,
        aai_product,
        '3'::varchar(10) as hierarchy_level,
        'DUCKCREEK'::varchar(20) as source_system_code,
        principal_coverage_indicator
    from cte_base
),

cte_all_nodes as (
    select * from cte_riskgroup
    union all
    select * from cte_risk
    union all
    select * from cte_coverage
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    source_system_code,
    dim_coverage_key,
    dim_coverage_key_parent,
    brand,
    jurisdiction,
    product,
    productline,
    levelofcover,
    risktype,
    coveragetype,
    producttype,
    aai_product,
    hierarchy_level,
    principal_coverage_indicator
from cte_all_nodes
