-- join voterfile information with donor table for final targeting model table
create or replace table `nyu-cap-ae.final.targeting_model_data` as

with voters as (
    select
        b.donor_id,
        b.FIRST_NAME,
        b.LAST_NAME,
        b.RESIDENTIAL_CITY,
        b.RESIDENTIAL_STATE,
        b.RESIDENTIAL_ZIP,
        b.age as voter_age,
        b.PARTY_AFFILIATION as voter_party_affiliation,
        b.GENERAL_2000_11_07,
        b.GENERAL_2001_11_06,
        b.GENERAL_2002_11_05,
        b.GENERAL_2003_11_04,
        b.GENERAL_2004_11_02,
        b.GENERAL_2005_11_08,
        b.GENERAL_2006_11_07,
        b.GENERAL_2007_11_06,
        b.GENERAL_2008_11_04,
        b.GENERAL_2008_11_18,
        b.GENERAL_2009_11_03,
        b.GENERAL_2010_11_02,
        b.GENERAL_2011_11_08,
        b.GENERAL_2012_11_06,
        b.GENERAL_2013_11_05,
        b.GENERAL_2014_11_04,
        b.GENERAL_2015_11_03,
        b.GENERAL_2016_11_08,
        b.GENERAL_2017_11_07,
        b.GENERAL_2018_11_06,
        b.GENERAL_2019_11_05,
        b.GENERAL_2020_11_03,
        row_number() over(partition by donor_id
        order by donor_id desc) as row_rank3
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
    , b.dollars_donated_2000_cycle
    , b.donations_made_2000_cycle
    , b.dollars_donated_2002_cycle
    , b.donations_made_2002_cycle
    , b.dollars_donated_2004_cycle
    , b.donations_made_2004_cycle
    , b.dollars_donated_2006_cycle
    , b.donations_made_2006_cycle
    , b.dollars_donated_2008_cycle
    , b.donations_made_2008_cycle
    , b.dollars_donated_2010_cycle
    , b.donations_made_2010_cycle
    , b.dollars_donated_2012_cycle
    , b.donations_made_2012_cycle
    , b.dollars_donated_2014_cycle
    , b.donations_made_2014_cycle
    , b.dollars_donated_2016_cycle
    , b.donations_made_2016_cycle
    , b.dollars_donated_2018_cycle
    , b.donations_made_2018_cycle
    , b.dollars_donated_2020_cycle
    , b.donations_made_2020_cycle
    , b.total_donations_made_all_time
    , b.total_dollars_all_time
    , b.republican_donations
    , b.republican_dollars
    , b.democrat_donations
    , b.democrat_dollars
    , b.house_donations
    , b.house_dollars
    , b.senate_donations
    , b.senate_dollars
    , b.treasurer_donations
    , b.treasurer_dollars
    , b.secretary_of_state_donations
    , b.secretary_of_state_dollars
    , b.governor_donations
    , b.governor_dollars
    , b.auditor_donations
    , b.auditor_dollars
    , b.attorney_general_donations
    , b.attorney_general_dollars
    , b.undeclared_donations
    , b.undeclared_dollars
    , b.other_dollars
    , b.other_donations
    , a.GENERAL_2000_11_07 as voted_2000
    , a.GENERAL_2001_11_06 as voted_2001
    , a.GENERAL_2002_11_05 as voted_2002
    , a.GENERAL_2003_11_04 as voted_2003
    , a.GENERAL_2004_11_02 as voted_2004
    , a.GENERAL_2005_11_08 as voted_2005
    , a.GENERAL_2006_11_07 as voted_2006
    , a.GENERAL_2007_11_06 as voted_2007
    , a.GENERAL_2008_11_04 as voted_2008
    , a.GENERAL_2009_11_03 as voted_2009
    , a.GENERAL_2010_11_02 as voted_2010
    , a.GENERAL_2011_11_08 as voted_2011
    , a.GENERAL_2012_11_06 as voted_2012
    , a.GENERAL_2013_11_05 as voted_2013
    , a.GENERAL_2014_11_04 as voted_2014
    , a.GENERAL_2015_11_03 as voted_2015
    , a.GENERAL_2016_11_08 as voted_2016
    , a.GENERAL_2017_11_07 as voted_2017
    , a.GENERAL_2018_11_06 as voted_2018
    , a.GENERAL_2019_11_05 as voted_2019
    , a.GENERAL_2020_11_03 as voted_2020
from (select * from voters where voters.row_rank3 = 1) as a
    left join `nyu-cap-ae.elt_ohio.donors` as b on a.donor_id = b.donor_id;

alter table `nyu-cap-ae.final.targeting_model_data` add column donor_status numeric;

update `nyu-cap-ae.final.targeting_model_data` set donor_status = 0
where total_dollars_all_time = 0 or total_dollars_all_time is null;

update `nyu-cap-ae.final.targeting_model_data` set donor_status = 1
where total_dollars_all_time > 0;
