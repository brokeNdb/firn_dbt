{{ config(materialized='table') }}

with base as (
    select
        lower(coalesce(src.employee_code, src.usrpfr)) as employee_code,
        src.start_date,
        src.end_date,
        src.name,
        split_part(src.name, ', ', 2) as first_name,
        split_part(src.name, ', ', 1) as last_name,
        src.group_1,
        src.group_2,
        src.group_3,
        src.group_4,
        src.code::integer as code,
        src.lastchgdatetime as extract_timestamp,
        lag(src.start_date) over (
            partition by lower(coalesce(src.employee_code, src.usrpfr))
            order by src.start_date, src.end_date
        ) as start_date_lag,
        lag(src.end_date) over (
            partition by lower(coalesce(src.employee_code, src.usrpfr))
            order by src.start_date, src.end_date
        ) as end_date_lag,
        case
            when upper(substring(src.employee_code, 1, 1)) in ('U', 'R')
                and length(src.employee_code) = 7
                then substring(src.employee_code, 2)
            else src.employee_code
        end::char(8) as protect_id_join
    from {{ source('upstream', 'aai_nz_master') }} as src
    where src.versionnumber = (
        select max(versionnumber::int)
        from {{ source('upstream', 'aai_nz_master') }}
    )
),

fixed as (
    select
        employee_code,
        start_date,
        end_date,
        name,
        first_name,
        last_name,
        group_1,
        group_2,
        group_3,
        group_4,
        code,
        extract_timestamp,
        case
            when end_date_lag <= start_date then dateadd(day, 1, end_date_lag)
            when start_date <= end_date_lag then
                case
                    when end_date_lag = '9999-12-31' then '9999-12-31'::timestamp
                    else dateadd(day, 1, end_date_lag)
                end
            else start_date
        end as start_date_error_fix,
        protect_id_join
    from base
)

select
    employee_code,
    start_date_error_fix as start_date,
    to_char(start_date_error_fix, 'YYYYMMDD')::integer as start_date_integer,
    end_date,
    to_char(end_date, 'YYYYMMDD')::integer as end_date_integer,
    name,
    first_name,
    last_name,
    group_1,
    group_2,
    group_3,
    group_4,
    code,
    extract_timestamp,
    case
        when start_date_error_fix <> start_date then true
        else false
    end as start_date_fixed_flag,
    protect_id_join
from fixed
where employee_code is not null

union all

select
    'unknown' as employee_code,
    '1900-01-01'::timestamp as start_date,
    19900101::integer as start_date_integer,
    '9999-12-31'::timestamp as end_date,
    99991231::integer as end_date_integer,
    'UNKNOWN' as name,
    'UNKNOWN' as first_name,
    'UNKNOWN' as last_name,
    'UNKNOWN' as group_1,
    'UNKNOWN' as group_2,
    'UNKNOWN' as group_3,
    'UNKNOWN' as group_4,
    -1 as code,
    current_timestamp() as extract_timestamp,
    false as start_date_fixed_flag,
    null::char(8) as protect_id_join
