-- join voterfile information with donor table for final targeting model table
create or replace table `nyu-cap-ae.final.tbl_ohio_targeting_model_input` as

with voters as (
    select
        b.donor_id,
        b.FIRST_NAME,
        b.LAST_NAME,
        b.RESIDENTIAL_CITY,
        b.RESIDENTIAL_STATE,
        b.RESIDENTIAL_ZIP,
        b.AGE as voter_age,
        b.PARTY_AFFILIATION as voter_party_affiliation,
        b.GENERAL_2000,
        b.GENERAL_2001,
        b.GENERAL_2002,
        b.GENERAL_2003,
        b.GENERAL_2004,
        b.GENERAL_2005,
        b.GENERAL_2006,
        b.GENERAL_2007,
        b.GENERAL_2008,
        b.GENERAL_2009,
        b.GENERAL_2010,
        b.GENERAL_2011,
        b.GENERAL_2012,
        b.GENERAL_2013,
        b.GENERAL_2014,
        b.GENERAL_2015,
        b.GENERAL_2016,
        b.GENERAL_2017,
        b.GENERAL_2018,
        b.GENERAL_2019,
        b.GENERAL_2020,
        b.PRIMARY_2000,
        b.PRIMARY_2002,
        b.PRIMARY_2004,
        b.PRIMARY_2005,
        b.PRIMARY_2006,
        b.PRIMARY_2007,
        b.PRIMARY_2008,
        b.PRIMARY_2009,
        b.PRIMARY_2010,
        b.PRIMARY_2011,
        b.PRIMARY_2012,
        b.PRIMARY_2013,
        b.PRIMARY_2014,
        b.PRIMARY_2015,
        b.PRIMARY_2016,
        b.PRIMARY_2017,
        b.PRIMARY_2018,
        b.PRIMARY_2019,
        b.PRIMARY_2020,
        row_number() over(partition by donor_id order by donor_id desc) as row_rank3
    from `nyu-cap-ae.elt_ohio.voterfiles` as b
)

select
    a.donor_id
    , a.FIRST_NAME as voter_first_name
    , a.LAST_NAME as voter_last_name
    , b.donor_occupation
    , b.donor_profession
    , a.voter_age
    , a.voter_party_affiliation
    , a.RESIDENTIAL_CITY as voter_city
    , a.RESIDENTIAL_STATE as voter_state
    , a.RESIDENTIAL_ZIP as voter_zip
    , b.total_dollars_all_time
    , a.GENERAL_2000,
    a.GENERAL_2001,
    a.GENERAL_2002,
    a.GENERAL_2003,
    a.GENERAL_2004,
    a.GENERAL_2005,
    a.GENERAL_2006,
    a.GENERAL_2007,
    a.GENERAL_2008,
    a.GENERAL_2009,
    a.GENERAL_2010,
    a.GENERAL_2011,
    a.GENERAL_2012,
    a.GENERAL_2013,
    a.GENERAL_2014,
    a.GENERAL_2015,
    a.GENERAL_2016,
    a.GENERAL_2017,
    a.GENERAL_2018,
    a.GENERAL_2019,
    a.GENERAL_2020,
    a.PRIMARY_2000,
    a.PRIMARY_2002,
    a.PRIMARY_2004,
    a.PRIMARY_2005,
    a.PRIMARY_2006,
    a.PRIMARY_2007,
    a.PRIMARY_2008,
    a.PRIMARY_2009,
    a.PRIMARY_2010,
    a.PRIMARY_2011,
    a.PRIMARY_2012,
    a.PRIMARY_2013,
    a.PRIMARY_2014,
    a.PRIMARY_2015,
    a.PRIMARY_2016,
    a.PRIMARY_2017,
    a.PRIMARY_2018,
    a.PRIMARY_2019,
    a.PRIMARY_2020
from (select * from voters where voters.row_rank3 = 1) as a
    left join `nyu-cap-ae.elt_ohio.donors` as b on a.donor_id = b.donor_id;

alter table `nyu-cap-ae.final.tbl_ohio_targeting_model_input` add column donor_status numeric;

update `nyu-cap-ae.final.tbl_ohio_targeting_model_input` set donor_status = 0
where total_dollars_all_time = 0 or total_dollars_all_time is null;

update `nyu-cap-ae.final.tbl_ohio_targeting_model_input` set donor_status = 1
where total_dollars_all_time > 0;
