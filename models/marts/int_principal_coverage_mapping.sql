{{ config(materialized='view') }}

select 'Comprehensive' as levelofcover, 'Motorcar' as risktype, 'Own Property Damage' as coveragetype, 1 as principal_coverage_indicator
union all
select 'Third Party Fire and Theft', 'Motorcar', 'Fire and Theft', 1
union all
select 'Third Party Property Damage', 'Vehicle Liability', 'Third Party Property Damage', 1
union all
select 'Landlord Building', 'Residential Property', 'Defined Events', 1
union all
select 'Landlord Building and Contents', 'Residential Property', 'Defined Events', 1
union all
select 'Building', 'Residential Property', 'Defined Events', 1
union all
select 'Building and Contents', 'Residential Property', 'Defined Events', 1
union all
select 'Building and Limited Contents', 'Residential Property', 'Defined Events', 1
union all
select 'Building and Contents', 'Property Contents', 'Defined Events', 1
union all
select 'Building and Limited Contents', 'Property Contents', 'Defined Events', 1
union all
select 'Contents', 'Property Contents', 'Defined Events', 1
union all
select 'Limited Contents', 'Property Contents', 'Defined Events', 1
