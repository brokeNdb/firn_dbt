{{ config(materialized='table') }}

with product_level as (
    select
        pc.dim_coverage_key,
        case
            when pc.productline = 'Motorcar' then 'Motor'
            when pc.levelofcover in ('Building', 'Building and Contents', 'Building and Limited Contents', 'Landlord Building', 'Landlord Building and Contents') then 'Home'
        end as product,
        case
            when pc.levelofcover = 'Comprehensive' and pc.productline = 'Motorcar' then 'Motor Comprehensive'
            when pc.levelofcover = 'Third Party Property Damage' and pc.productline = 'Motorcar' then 'Motor Third Party Property Damage'
            when pc.levelofcover = 'Third Party Fire and Theft' and pc.productline = 'Motorcar' then 'Motor Third Party Fire and Theft'
            when pc.levelofcover in ('Building', 'Building and Contents', 'Building and Limited Contents') then 'Home'
            when pc.levelofcover in ('Landlord Building', 'Landlord Building and Contents') then 'Landlord'
        end as sub_product,
        pc.levelofcover,
        pc.productline
    from {{ ref('dim_product_coverage') }} as pc
    where pc.producttype = 'RiskGroup'
      and pc.levelofcover is not null

    union

    select
        pc.dim_coverage_key,
        case
            when pc.levelofcover in ('Contents', 'Building and Contents', 'Limited Contents', 'Building and Limited Contents') then 'Content'
        end as product,
        case
            when pc.levelofcover in ('Contents', 'Building and Contents') then 'Contents'
            when pc.levelofcover in ('Limited Contents', 'Building and Limited Contents') then 'Limited Contents'
        end as sub_product,
        pc.levelofcover,
        pc.productline
    from {{ ref('dim_product_coverage') }} as pc
    where pc.producttype = 'RiskGroup'
      and pc.levelofcover is not null
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'DUCKCREEK' as source_system_code,
    p.dim_coverage_key,
    p.product,
    p.sub_product,
    p.levelofcover,
    p.productline
from product_level as p
where product is not null
