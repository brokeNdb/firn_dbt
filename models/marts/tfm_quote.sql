{{ config(materialized='table') }}

with quote_tx as (
    select
        q.policy_key,
        q.start_date,
        q.policy_number,
        q.jurisdiction_state as jurisdiction
    from {{ source('upstream', 'vw_v_quotes_all') }} as q
    group by 1, 2, 3, 4
),

q_extra as (
    select
        qt.policy_key,
        qt.start_date,
        max(case when d.normalized_name = 'LastTransactionStatusUser' then coalesce(f.value_string, f.value_number::varchar(200)) end) as lasttransactionstatususer,
        max(case when d.normalized_name = 'QuoteTriggerCode' then coalesce(f.value_string, f.value_number::varchar(200)) end) as quotetriggercode,
        max(case when d.normalized_name = 'InceptionTechnologyInteractionChannel' then coalesce(f.value_string, f.value_number::varchar(200)) end) as inceptiontechnologyinteractionchannel,
        max(case when d.normalized_name = 'Brand' then coalesce(f.value_string, f.value_number::varchar(200)) end) as brand,
        max(case when d.normalized_name = 'Product' then coalesce(f.value_string, f.value_number::varchar(200)) end) as product
    from quote_tx as qt
    inner join {{ source('upstream', 'int_extra_attribute_value_quote') }} as f
        on qt.policy_key = f.policy_key
       and qt.start_date = f.start_date
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on f.attribute_sk = d.attribute_sk
       and d.entity_type = 'quotes'
       and d.raw_view_name = 'vw_v_quotes_extra_data'
    where d.normalized_name in ('LastTransactionStatusUser', 'QuoteTriggerCode', 'InceptionTechnologyInteractionChannel', 'Brand', 'Product')
    group by 1, 2
),

q_line as (
    select
        qt.policy_key,
        qt.start_date,
        max(ln.line_column_value) as productline
    from quote_tx as qt
    inner join {{ source('upstream', 'vw_v_quotes_line') }} as ln
        on qt.policy_key = ln.policy_key
       and qt.start_date = ln.start_date
    where ln.line_column_name = 'ProductLine'
    group by 1, 2
),

io_vals as (
    select
        qt.policy_key,
        qt.start_date,
        max(case when d.normalized_name = 'Campaign Code' then coalesce(f.value_string, f.value_number::varchar(200)) end) as campaign_code,
        max(case when d.normalized_name = 'SumInsured' then coalesce(f.value_string, f.value_number::varchar(200)) end) as suminsured_raw,
        max(case when d.normalized_name = 'LevelOfCover' then coalesce(f.value_string, f.value_number::varchar(200)) end) as levelofcover
    from quote_tx as qt
    inner join {{ source('upstream', 'vw_v_quotes_insured_object') }} as io
        on qt.policy_key = io.policy_key
       and qt.start_date = io.start_date
       and io.insured_object_type in ('Risk', 'RiskGroup', 'Location', 'Asset', 'PromiseDetail')
    inner join {{ source('upstream', 'int_extra_attribute_value_quote_insured_object') }} as f
        on io.policy_key = f.policy_key
       and io.start_date = f.start_date
       and io.insured_object_key = f.insured_object_key
    inner join {{ source('upstream', 'dim_extra_attribute') }} as d
        on f.attribute_sk = d.attribute_sk
       and d.entity_type = 'quotes_insured_object'
       and d.raw_view_name = 'vw_v_quotes_insured_object_extra_data'
    where d.normalized_name in ('Campaign Code', 'SumInsured', 'LevelOfCover')
    group by 1, 2
),

first_start as (
    select
        policy_key,
        min(start_date) as first_start_date
    from quote_tx
    group by 1
),

channel_map as (
    select * from {{ ref('int_channel_mapping') }}
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'DUCKCREEK' as source_system_code,
    tx.policy_key,
    max(tx.policy_number) as policy_number,
    min(tx.start_date) as quote_date,
    max(io.campaign_code) as campaign_code,
    max(case when qe.quotetriggercode = 'QuoteRated' then 'Y' else null end) as quoteratedflag,
    max(case when qe.quotetriggercode = 'QuoteRated' then tx.start_date else null end) as quoterateddate,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then 'Y' else null end) as quotecompletedflag,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then tx.start_date else null end) as quotecompleteddate,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then ch.normalized_channel end) as channel,
    max(case when tx.start_date = fs.first_start_date then ch.normalized_channel end) as channel_start_at,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then qe.brand end) as brand,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then tx.jurisdiction end) as jurisdiction,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then qe.product end) as product,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then ql.productline end) as productline,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then io.levelofcover end) as levelofcover,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then io.suminsured_raw end) as suminsured_raw,
    max(case when qe.quotetriggercode = 'QuoteCompleted' then qe.lasttransactionstatususer end) as userid,
    max(fs.first_start_date) as first_start_date
from quote_tx as tx
inner join first_start as fs
    on tx.policy_key = fs.policy_key
left join q_extra as qe
    on tx.policy_key = qe.policy_key
   and tx.start_date = qe.start_date
left join q_line as ql
    on tx.policy_key = ql.policy_key
   and tx.start_date = ql.start_date
left join io_vals as io
    on tx.policy_key = io.policy_key
   and tx.start_date = io.start_date
left join channel_map as ch
    on qe.inceptiontechnologyinteractionchannel = ch.raw_interaction_value
group by 1, 2, 3, 4
