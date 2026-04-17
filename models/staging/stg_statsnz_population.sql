with source as (

    select *
    from FIRN_PROJECT.LANDING.STATSNZ_POPULATION_TABLE

)

select
    name as area_name,
    erp18 as estimated_resident_population_2018,
    erp19 as estimated_resident_population_2019,
    erp20 as estimated_resident_population_2020,
    erp21 as estimated_resident_population_2021,
    avann1820num as average_annual_change_2018_2020_num,
    avann1820per as average_annual_change_2018_2020_pct,
    chg2021num as change_2021_num,
    chg2021per as change_2021_pct
from source
