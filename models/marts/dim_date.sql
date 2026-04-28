{{ config(materialized='table') }}

with ref_holidays as (
    select 20100101 as cal_date_key, 'New Years Day Friday' as holiday_name union all
    select 20100102, 'Day after New Years Day - Saturday' union all
    select 20100104, 'Day after New Years Day - Observed' union all
    select 20100206, 'Waitangi Day' union all
    select 20100402, 'Good Friday' union all
    select 20100405, 'Easter Monday' union all
    select 20100425, 'ANZAC Day' union all
    select 20100607, 'Queens Birthday' union all
    select 20101025, 'Labour Day' union all
    select 20101225, 'Christmas Day - Saturday' union all
    select 20101227, 'Christmas Day - Observed' union all
    select 20101226, 'Boxing Day - Sunday' union all
    select 20101228, 'Boxing Day - Observed' union all
    select 20110101, 'New Years Day - Saturday' union all
    select 20110102, 'Day after New Years Day - Sunday' union all
    select 20110103, 'New Years Day - Observed' union all
    select 20110104, 'Day after New Years Day - Observed' union all
    select 20110206, 'Waitangi Day' union all
    select 20110422, 'Good Friday' union all
    select 20110425, 'Easter Monday & ANZAC Day' union all
    select 20110606, 'Queens Birthday' union all
    select 20111024, 'Labour Day' union all
    select 20111225, 'Christmas Day - Sunday' union all
    select 20111226, 'Boxing Day' union all
    select 20111227, 'Christmas Day - Observed' union all
    select 20120101, 'New Years Day - Sunday' union all
    select 20120102, 'Day after New Years Day' union all
    select 20120103, 'New Years Day - Observed' union all
    select 20120206, 'Waitangi Day' union all
    select 20120406, 'Good Friday' union all
    select 20120409, 'Easter Monday' union all
    select 20120425, 'ANZAC Day' union all
    select 20120604, 'Queens Birthday' union all
    select 20121022, 'Labour Day' union all
    select 20121225, 'Christmas Day' union all
    select 20121226, 'Boxing Day' union all
    select 20130101, 'New Years Day' union all
    select 20130102, 'Day after New Years Day' union all
    select 20130206, 'Waitangi Day' union all
    select 20130329, 'Good Friday' union all
    select 20130401, 'Easter Monday' union all
    select 20130425, 'ANZAC Day' union all
    select 20130603, 'Queens Birthday' union all
    select 20131028, 'Labour Day' union all
    select 20131225, 'Christmas Day' union all
    select 20131226, 'Boxing Day' union all
    select 20140101, 'New Years Day' union all
    select 20140102, 'Day after New Years Day' union all
    select 20140206, 'Waitangi Day' union all
    select 20140418, 'Good Friday' union all
    select 20140421, 'Easter Monday' union all
    select 20140425, 'ANZAC Day' union all
    select 20140602, 'Queens Birthday' union all
    select 20141027, 'Labour Day' union all
    select 20141225, 'Christmas Day' union all
    select 20141226, 'Boxing Day' union all
    select 20150101, 'New Years Day' union all
    select 20150102, 'Day after New Years Day' union all
    select 20150206, 'Waitangi Day' union all
    select 20150403, 'Good Friday' union all
    select 20150406, 'Easter Monday' union all
    select 20150425, 'ANZAC Day - Saturday' union all
    select 20150427, 'ANZAC Day - Observed' union all
    select 20150601, 'Queens Birthday' union all
    select 20151026, 'Labour Day' union all
    select 20151225, 'Christmas Day' union all
    select 20151226, 'Boxing Day - Saturday' union all
    select 20151228, 'Boxing Day - Observed' union all
    select 20160101, 'New Years Day' union all
    select 20160102, 'Day after New Years Day - Saturday' union all
    select 20160104, 'Day after New Years Day - Observed' union all
    select 20160206, 'Waitangi Day - Saturday' union all
    select 20160208, 'Waitangi Day - Observed' union all
    select 20160325, 'Good Friday' union all
    select 20160328, 'Easter Monday' union all
    select 20160425, 'ANZAC Day' union all
    select 20160606, 'Queens Birthday' union all
    select 20161024, 'Labour Day' union all
    select 20161225, 'Christmas Day - Sunday' union all
    select 20161226, 'Boxing Day' union all
    select 20161227, 'Christmas Day - Observed' union all
    select 20170101, 'New Years Day - Sunday ' union all
    select 20170102, 'Day after New Years Day' union all
    select 20170103, 'New Years Day - Observed' union all
    select 20170206, 'Waitangi Day' union all
    select 20170414, 'Good Friday' union all
    select 20170417, 'Easter Monday' union all
    select 20170425, 'ANZAC Day' union all
    select 20170605, 'Queens Birthday' union all
    select 20171023, 'Labour Day' union all
    select 20171225, 'Christmas Day' union all
    select 20171226, 'Boxing Day' union all
    select 20180101, 'New Years Day - Sunday ' union all
    select 20180102, 'Day after New Years Day' union all
    select 20180206, 'Waitangi Day' union all
    select 20180330, 'Good Friday' union all
    select 20180402, 'Easter Monday' union all
    select 20180425, 'ANZAC Day' union all
    select 20180604, 'Queens Birthday' union all
    select 20181022, 'Labour Day' union all
    select 20181225, 'Christmas Day' union all
    select 20181226, 'Boxing Day' union all
    select 20190101, 'New Years Day - Sunday ' union all
    select 20190102, 'Day after New Years Day' union all
    select 20190206, 'Waitangi Day' union all
    select 20190419, 'Good Friday' union all
    select 20190422, 'Easter Monday' union all
    select 20190425, 'ANZAC Day' union all
    select 20190603, 'Queens Birthday' union all
    select 20191028, 'Labour Day' union all
    select 20191225, 'Christmas Day' union all
    select 20191226, 'Boxing Day' union all
    select 20200101, 'New Years Day - Wednesday' union all
    select 20200102, 'Day after New Years Day' union all
    select 20200206, 'Waitangi Day' union all
    select 20200410, 'Good Friday' union all
    select 20200413, 'Easter Monday' union all
    select 20200425, 'ANZAC Day - Saturday' union all
    select 20200427, 'ANZAC Day - Observed' union all
    select 20200601, 'Queens Birthday' union all
    select 20201026, 'Labour Day' union all
    select 20201225, 'Christmas Day' union all
    select 20201226, 'Boxing Day - Saturday' union all
    select 20201228, 'Boxing Day - Observed' union all
    select 20210101, 'New Years Day - Friday ' union all
    select 20210102, 'Day after New Years Day' union all
    select 20210104, 'Day after New Years Day - Observed' union all
    select 20210206, 'Waitangi Day' union all
    select 20210208, 'Waitangi Day - Observed' union all
    select 20210402, 'Good Friday' union all
    select 20210405, 'Easter Monday' union all
    select 20210425, 'ANZAC Day' union all
    select 20210426, 'ANZAC Day - Observed' union all
    select 20210607, 'Queens Birthday' union all
    select 20211025, 'Labour Day' union all
    select 20211225, 'Christmas Day' union all
    select 20211226, 'Boxing Day - Saturday' union all
    select 20211227, 'Christmas Day' union all
    select 20211228, 'Boxing Day - Observed' union all
    select 20220101, 'New Years Day - Saturday ' union all
    select 20220103, 'New Years Day - Monday ' union all
    select 20220102, 'Day after New Years Day' union all
    select 20220104, 'Day after New Years Day - Observed' union all
    select 20220206, 'Waitangi Day' union all
    select 20220207, 'Waitangi Day - Observed' union all
    select 20220415, 'Good Friday' union all
    select 20220418, 'Easter Monday' union all
    select 20220425, 'ANZAC Day' union all
    select 20220606, 'Queens Birthday' union all
    select 20220624, 'Matariki' union all
    select 20221024, 'Labour Day' union all
    select 20221225, 'Christmas Day' union all
    select 20221226, 'Boxing Day - Monday' union all
    select 20221227, 'Christmas Day' union all
    select 20230101, 'New Years Day' union all
    select 20230102, 'Day after New Years Day' union all
    select 20230103, 'New Years Day - Observed' union all
    select 20230206, 'Waitangi Day' union all
    select 20230407, 'Good Friday' union all
    select 20230410, 'Easter Monday' union all
    select 20230425, 'ANZAC Day' union all
    select 20230605, 'Queens Birthday' union all
    select 20230714, 'Matariki' union all
    select 20231023, 'Labour Day' union all
    select 20231225, 'Christmas Day' union all
    select 20231226, 'Boxing Day' union all
    select 20240101, 'New Years Day' union all
    select 20240102, 'Day after New Years Day' union all
    select 20240206, 'Waitangi Day' union all
    select 20240329, 'Good Friday' union all
    select 20240401, 'Easter Monday' union all
    select 20240425, 'ANZAC Day' union all
    select 20240603, 'Queens Birthday' union all
    select 20240628, 'Matariki' union all
    select 20241028, 'Labour Day' union all
    select 20241225, 'Christmas Day' union all
    select 20241226, 'Boxing Day' union all
    select 20250101, 'New Years Day' union all
    select 20250102, 'Day after New Years Day' union all
    select 20250206, 'Waitangi Day' union all
    select 20250418, 'Good Friday' union all
    select 20250421, 'Easter Monday' union all
    select 20250425, 'ANZAC Day' union all
    select 20250602, 'Kings Birthday' union all
    select 20250620, 'Matariki' union all
    select 20251027, 'Labour Day' union all
    select 20251225, 'Christmas Day' union all
    select 20251226, 'Boxing Day'
),

dates as (
    select
        to_number(to_char(dateadd(day, seq4(), to_date('1900-01-01')), 'YYYYMMDD')) as cal_date_key,
        dateadd(day, seq4(), to_date('1900-01-01')) as cal_date,
        to_number(to_char(dateadd(day, seq4() + 1, to_date('1900-01-01')), 'YYYYMMDD')) as next_cal_date_key
    from table(generator(rowcount => 109574))

    union all

    select 99991231, to_date('9999-12-31'), 99999999
),

dim_date_base as (
    select
        d.cal_date_key,
        d.cal_date,
        d.next_cal_date_key,
        iff(h.cal_date_key is null, false, true) as holiday_flag
    from dates as d
    left join ref_holidays as h
        on d.cal_date_key = h.cal_date_key
),

current_date_cte as (
    select
        current_date() as cur_date,
        to_number(to_char(current_date(), 'YYYYMMDD')) as cur_date_key,
        dateadd(week, -1, current_date()) as lweek_date,
        dateadd(month, -1, current_date()) as lmnth_date,
        dateadd(quarter, -1, current_date()) as lqtr_date,
        dateadd(year, -1, current_date()) as lyear_date,
        date_part(year, current_date()) as cur_year,
        date_part(year, current_date()) - iff(date_part(month, current_date()) <= 6, 1, 0) as cur_fin_year
),

enriched as (
    select
        dim.cal_date_key,
        dim.cal_date,
        dim.next_cal_date_key,
        dim.holiday_flag,
        to_number(to_char(dim.cal_date, 'YYYYDDD')) as cal_date_julian,
        to_number(to_char(dim.cal_date, 'DDD')) as cal_julian_day,
        to_number(to_char(dim.cal_date, 'YYYYWW')) as cal_year_week,
        to_number(to_char(dim.cal_date, 'WW')) as cal_week,
        to_number(to_char(dim.cal_date, 'YYYYMM')) as cal_year_month,
        to_number(to_char(dim.cal_date, 'MM')) as cal_month,
        to_number(to_char(dim.cal_date, 'Q')) as cal_quarter,
        to_number(to_char(dim.cal_date, 'YYYY')) as cal_year,
        to_number(to_char(dim.cal_date, 'DD')) as day_of_month,
        to_number(to_char(dim.cal_date, 'DDD')) as day_of_year,
        to_number(to_char(dateadd(day, -1, dim.cal_date), 'D')) as day_of_week,
        to_varchar(to_char(dim.cal_date, 'DAY')) as day_of_week_name,
        to_varchar(to_char(dim.cal_date, 'DY')) as day_of_week_abbrev,
        extract(day from last_day(dim.cal_date)) as days_in_month,
        to_number(to_char(dim.cal_date, 'W')) as week_of_month,
        to_number(to_char(dim.cal_date, 'WW')) as week_of_year,
        to_number(to_char(dim.cal_date, 'MM')) as month_of_year,
        to_varchar(to_char(dim.cal_date, 'MONTH')) as month_of_year_name,
        to_varchar(to_char(dim.cal_date, 'MON')) as month_of_year_abbrev,
        to_number(to_char(last_day(dim.cal_date), 'YYYYMMDD')) as eom_date_key,
        to_number(to_char(date_trunc('month', dim.cal_date), 'YYYYMMDD')) as month_start_key,
        to_number(to_char(last_day(dim.cal_date), 'YYYYMMDD')) as month_end_key,
        iff(to_char(dim.cal_date, 'DY') in ('SAT', 'SUN'), true, false) as weekend_flag,
        dateadd(day,
            case when dayofweek(dim.cal_date) = 0 then -6 else 1 - dayofweek(dim.cal_date) end,
            dim.cal_date
        )::date as week_start_monday,
        date_part(year, dim.cal_date) + iff(date_part(month, dim.cal_date) > 6, 1, 0) as fin_year,
        iff(date_part(month, dim.cal_date) > 6, date_part(month, dim.cal_date) - 6, date_part(month, dim.cal_date) + 6) as fin_month
    from dim_date_base as dim
)

select
    current_timestamp() as edw_load_timestamp,
    'REPLACE_WITH_EDW_MDF_AUDIT_ID'::varchar(100) as edw_mdf_audit_id,
    'REF' as source_system_code,
    e.cal_date_key as dim_date_key,
    e.cal_date_key as business_key,
    e.cal_date::timestamp as cal_date,
    e.cal_date_key,
    e.next_cal_date_key,
    e.cal_date_julian,
    e.cal_julian_day,
    e.cal_year_week,
    e.cal_week,
    e.cal_year_month,
    e.cal_month,
    to_number(e.cal_year || lpad(e.cal_quarter::varchar, 2, '0')) as cal_year_quarter,
    to_number(lpad(e.cal_quarter::varchar, 2, '0')) as cal_quarter,
    e.cal_year,
    to_number(e.fin_year || lpad(e.fin_month::varchar, 2, '0')) as fin_year_month,
    e.fin_month,
    to_number(e.fin_year || lpad(date_part(quarter, date_from_parts(e.cal_year, e.cal_month, 1) + iff(e.cal_month <= 6, 184, -181))::varchar, 2, '0')) as fin_year_quarter,
    to_number(lpad(date_part(quarter, date_from_parts(e.cal_year, e.cal_month, 1) + iff(e.cal_month <= 6, 184, -181))::varchar, 2, '0')) as fin_quarter,
    e.fin_year,
    e.day_of_year,
    e.day_of_month,
    e.day_of_week,
    e.day_of_week_name,
    e.day_of_week_abbrev,
    e.days_in_month,
    e.week_of_month,
    e.week_of_year,
    e.month_of_year,
    e.month_of_year_name,
    e.month_of_year_abbrev,
    e.eom_date_key,
    e.weekend_flag,
    e.holiday_flag,
    iff(e.cal_date = cur.cur_date, true, false) as current_day_flag,
    iff(to_char(e.cal_date, 'YYYYWW') = to_char(cur.cur_date, 'YYYYWW'), true, false) as current_week_flag,
    iff(to_char(e.cal_date, 'YYYYMM') = to_char(cur.cur_date, 'YYYYMM'), true, false) as current_month_flag,
    iff(to_char(e.cal_date, 'YYYYQ') = to_char(cur.cur_date, 'YYYYQ'), true, false) as current_quarter_flag,
    iff(to_char(e.cal_date, 'YYYY') = to_char(cur.cur_date, 'YYYY'), true, false) as current_year_flag,
    iff(to_char(e.cal_date, 'YYYYWW') = to_char(cur.lweek_date, 'YYYYWW'), true, false) as previous_week_flag,
    iff(to_char(e.cal_date, 'YYYYMM') = to_char(cur.lmnth_date, 'YYYYMM'), true, false) as previous_month_flag,
    iff(to_char(e.cal_date, 'YYYYQ') = to_char(cur.lqtr_date, 'YYYYQ'), true, false) as previous_quarter_flag,
    iff(to_char(e.cal_date, 'YYYY') = to_char(cur.lyear_date, 'YYYY'), true, false) as previous_year_flag,
    iff(to_char(e.cal_date, 'DY') = 'MON', true, false) as eow_sun_flag,
    iff(to_char(e.cal_date, 'DY') = 'SAT', true, false) as eow_fri_flag,
    iff(e.cal_date = last_day(e.cal_date), true, false) as eom_flag,
    iff(to_char(e.cal_date, 'MMDD') = '1231', true, false) as eoy_flag,
    iff(to_char(e.cal_date, 'YYYYMM') = to_char(cur.cur_date, 'YYYYMM') and e.cal_date <= cur.cur_date, true, false) as mtd_flag,
    iff(to_char(e.cal_date, 'YYYYMM') = to_char(cur.lmnth_date, 'YYYYMM') and e.cal_date <= cur.lmnth_date, true, false) as lmtd_flag,
    iff(to_char(e.cal_date, 'YYYYMM') = to_char(cur.lyear_date, 'YYYYMM') and e.cal_date <= cur.lyear_date, true, false) as lymtd_flag,
    iff(to_char(e.cal_date, 'YYYYQ') = to_char(cur.cur_date, 'YYYYQ') and e.cal_date <= cur.cur_date, true, false) as qtd_flag,
    iff(to_char(e.cal_date, 'YYYYQ') = to_char(cur.lyear_date, 'YYYYQ') and e.cal_date <= cur.lyear_date, true, false) as lyqtd_flag,
    iff(to_char(e.cal_date, 'YYYY') = to_char(cur.cur_date, 'YYYY') and e.cal_date <= cur.cur_date, true, false) as ytd_flag,
    iff(to_char(e.cal_date, 'YYYY') = to_char(cur.lyear_date, 'YYYY') and e.cal_date <= cur.lyear_date, true, false) as lytd_flag,
    iff(e.fin_year = cur.cur_fin_year and e.cal_date <= cur.cur_date, true, false) as fin_ytd_flag,
    iff(e.fin_year = cur.cur_fin_year - 1 and e.cal_date <= cur.lyear_date, true, false) as fin_lytd_flag,
    iff(to_char(e.cal_date, 'YYYYMM') < to_char(cur.cur_date, 'YYYYMM'), true, false) as complete_month_flag,
    datediff(day, e.cal_date, cur.cur_date) as rolling_day,
    datediff(week, e.cal_date, cur.cur_date) as rolling_week,
    datediff(month, e.cal_date, cur.cur_date) as rolling_month,
    date_trunc('month', e.cal_date)::date as month_start,
    last_day(e.cal_date) as month_end,
    iff(e.cal_date = dateadd(day, -1, cur.cur_date), true, false) as previous_day_flag,
    e.week_start_monday
from enriched as e
cross join current_date_cte as cur
