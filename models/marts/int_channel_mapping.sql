{{ config(materialized='view') }}

select 'Phone' as raw_interaction_value, 'Assisted' as normalized_channel
union all
select 'Online Kiosk' as raw_interaction_value, 'NZAA' as normalized_channel
union all
select 'Online Browser' as raw_interaction_value, 'Online' as normalized_channel
