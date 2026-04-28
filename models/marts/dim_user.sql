{{ config(materialized='table') }}

-- TODO: fixes required as policy_id and username doesn't match to source reference SQL.
with cte_duckcreek_user as (
    select
        u.email,
        u.fullname,
        u.username,
        u.userid,
        coalesce(
            nullif(regexp_substr(upper(trim(u.fullname)), '[U][0-9]+'), ''),
            case when u.fullname = 'Express Admin' then 'ADMIN' else null end
        ) as unumber
    from {{ source('upstream', 'vw_v_platform_x_src_dc_plt_users') }} as u
    qualify row_number() over (
        partition by unumber
        order by u.extract_start_datetime desc, u.source_extract_datetime desc, u.cdc_start desc, u.ods_load_ts desc
    ) = 1
),

cte_master as (
    select
        tfm.code::bigint as dim_employee_key,
        (coalesce(tfm.employee_code::varchar, 'NULL') || coalesce(tfm.start_date_integer::varchar, 'NULL'))::varchar(36) as business_key,
        tfm.employee_code,
        tfm.protect_id_join::varchar(10) as protect_id,
        null::varchar(50) as ccr_id,
        duck.unumber as duck_unumber,
        duck.username as duck_username,
        tfm.start_date,
        tfm.start_date_integer,
        tfm.end_date,
        tfm.end_date_integer,
        tfm.name,
        tfm.first_name,
        tfm.last_name,
        tfm.group_1,
        tfm.group_2,
        tfm.group_3,
        tfm.group_4,
        tfm.extract_timestamp,
        tfm.start_date_fixed_flag,
        lower(tfm.employee_code) as l_employee_code,
        lower(tfm.group_4) as l_group_4
    from {{ ref('tfm_user') }} as tfm
    left join cte_duckcreek_user as duck
        on duck.unumber is not null
       and duck.unumber <> ''
       and lower(tfm.employee_code) = lower(duck.unumber)
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'DUCKCREEK' as source_system_code,
    rep.dim_employee_key,
    rep.business_key,
    rep.employee_code,
    rep.protect_id,
    rep.ccr_id,
    rep.duck_unumber,
    rep.duck_username,
    rep.start_date,
    rep.start_date_integer,
    dateadd(microsecond, -1, dateadd(day, 1, rep.end_date)) as end_date,
    rep.end_date_integer,
    rep.name,
    rep.first_name,
    rep.last_name,
    rep.group_1 as manager,
    rep.group_2 as location,
    rep.group_3 as location2,
    case
        when upper(coalesce(rep.group_3, 'NULL')) in ('ONLINE', 'NZAA') then rep.group_3
        when upper(rep.employee_code) = 'UNKNOWN' then 'UNKNOWN'
        else initcap('CONTACT CENTRE')
    end as channel,
    rep.extract_timestamp,
    rep.start_date_fixed_flag,
    coalesce(lead.dim_employee_key, rep.dim_employee_key) as team_leader_key,
    coalesce(lead.name, rep.group_4) as team_leader_name,
    coalesce(mgr.dim_employee_key, lead.dim_employee_key) as manager_key,
    coalesce(mgr.name, lead.group_4) as manager_name,
    coalesce(smgr.dim_employee_key, mgr.dim_employee_key) as senior_manager_key,
    coalesce(smgr.name, mgr.group_4) as senior_manager_name,
    smgr.group_1 as department,
    case
        when upper(smgr.group_1) in ('TECHNICAL OPERATIONS', 'PMO', 'SYSTEM PERFORMANCE') then true
        else false
    end as pvt_flag
from cte_master as rep
left join cte_master as lead
    on rep.l_group_4 = lead.l_employee_code
   and rep.end_date - 1 >= lead.start_date
   and rep.end_date <= lead.end_date
left join cte_master as mgr
    on lead.l_group_4 = mgr.l_employee_code
   and lead.end_date - 1 >= mgr.start_date
   and lead.end_date <= mgr.end_date
left join cte_master as smgr
    on mgr.l_group_4 = smgr.l_employee_code
   and mgr.end_date - 1 >= smgr.start_date
   and mgr.end_date <= smgr.end_date
