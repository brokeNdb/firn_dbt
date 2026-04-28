{{ config(materialized='table') }}

with blocked as (
    select distinct
        quoteid,
        visitdate
    from {{ source('upstream', 'vw_adan_aa_blocked') }}
    where quotecompletedflag = 1
      and nullif(quoteid, '') is not null
),

unblocked_ranked as (
    select
        quoteid,
        visitdate,
        case
            when count(quoteid) over (partition by visitdate, ipaddress) > 10 then '>10 QUOTE'
            else 'UNBLOCKED'
        end as blocked_category
    from {{ source('upstream', 'vw_adan_aa_unblocked') }} as u
    where nullif(quoteid, '') is not null
      and u.quotecompletedflag = 1
),

high_volume_unblocked as (
    select
        quoteid,
        visitdate
    from unblocked_ranked
    where blocked_category = '>10 QUOTE'
),

unioned as (
    select quoteid, visitdate from blocked
    union
    select quoteid, visitdate from high_volume_unblocked
)

select
    case
        when right(quoteid, 3) regexp '^[A-Za-z]+$' then left(quoteid, length(quoteid) - 3)
        else quoteid
    end as quote_number,
    visitdate::date as visitdate
from unioned
