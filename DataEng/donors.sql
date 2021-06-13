create or replace table `nyu-cap-ae.elt_ohio.donors` as

--add donor_id logic to all the contribution rows in the contribution table
with contributions_with_ids as (
    select
        *
        , LEFT(donor_first_name,3)||LEFT(donor_last_name,4)||donor_zip||left(donor_city, 1)||left(donor_original_address,2) as donor_id
    from
        `nyu-cap-ae.junk.individual_contributions_parsed_addresses`
),

-- for each donor_id, get aggregate donation information
donor_summary as (
    select
        donor_id,
        ifnull(count(contribution_amount), 0) as number_contributions,
        ifnull(sum(contribution_amount), 0) as total_contributions,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_party = 'REPUBLICAN' THEN contribution_amount END), 0) as republican_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_party = 'REPUBLICAN' THEN contribution_amount END), 0) as republican_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_party = 'DEMOCRAT' THEN contribution_amount END), 0) as democrat_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_party = 'DEMOCRAT' THEN contribution_amount END), 0) as democrat_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_party NOT IN ('REPUBLICAN', 'DEMOCRAT') THEN contribution_amount END), 0) as other_party_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_party NOT IN ('REPUBLICAN', 'DEMOCRAT') THEN contribution_amount END), 0) as other_party_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'HOUSE' THEN contribution_amount END), 0) as house_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'HOUSE' THEN contribution_amount END), 0) as house_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'SENATE' THEN contribution_amount END), 0) as senate_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'SENATE' THEN contribution_amount END), 0) as senate_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'TREASURER' THEN contribution_amount END), 0) as treasurer_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'TREASURER' THEN contribution_amount END), 0) as treasurer_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'SECRETARY_OF_STATE' THEN contribution_amount END), 0) as secretary_of_state_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'SECRETARY_OF_STATE' THEN contribution_amount END), 0) as secretary_of_state_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'GOVERNOR' THEN contribution_amount END), 0) as governor_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'GOVERNOR' THEN contribution_amount END), 0) as governor_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'AUDITOR' THEN contribution_amount END), 0) as auditor_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'AUDITOR' THEN contribution_amount END), 0) as auditor_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'ATTORNEY_GENERAL' THEN contribution_amount END), 0) as attorney_general_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'ATTORNEY_GENERAL' THEN contribution_amount END), 0) as attorney_general_dollars,
        ifnull(count(CASE WHEN contribution_amount > 0 and candidate_office = 'UNDECLARED_DONATIONS' THEN contribution_amount END), 0) as undeclared_donations,
        ifnull(sum(CASE WHEN contribution_amount > 0 and candidate_office = 'UNDECLARED_DONATIONS' THEN contribution_amount END), 0) as undeclared_dollars
    from contributions_with_ids
    group by 1
),

--dedupe donors with multiple occupations by taking the most recently reported occupation for each donor
donor_occupation_demo as (
    select
        left(donor_first_name,3)||left(donor_last_name,4)||donor_zip||left(donor_city, 1)||left(donor_original_address,2)  as donor_id,
        donor_occupation,
        row_number() over(partition by left(donor_first_name,3)||left(donor_last_name,4)||donor_zip||left(donor_city, 1)||left(donor_original_address,2)
            order by report_year desc) as row_rank
    from `nyu-cap-ae.junk.individual_contributions_parsed_addresses`
),

--dedupe donors with multiple addresses by taking the most recently reported address for each donor
donor_address_demo as (
    select
        left(donor_first_name,3)||left(donor_last_name,4)||donor_zip||left(donor_city, 1)||left(donor_original_address,2)  as donor_id
        , donor_first_name
        , donor_last_name
        , donor_original_address
        , address_number
        , street_name_pre_directional
        , street_name
        , street_name_post_type as street_name_type
        , street_name_post_directional
        , occupancy_type
        , occupancy_identifer
        , usps_box_id
        , donor_city
        , donor_state
        , donor_zip
        , donor_zip4
        , row_number() over(partition by left(donor_first_name,3)||left(donor_last_name,4)||donor_zip||left(donor_city, 1)||left(donor_original_address,2)
            order by report_year desc) as row_rank2
    from `nyu-cap-ae.junk.individual_contributions_parsed_addresses`
),

--get donation amounts for each donor for each year
donor_donations_by_year as (
    select
        left(donor_first_name,3)||left(donor_last_name,4)||donor_zip||left(donor_city, 1)||left(donor_original_address,2) as donor_id
        , cast(left(report_year,4) as numeric) as donation_year
        , sum(contribution_amount) as total_dollars_donated
        , count(distinct(contribution_id)) as total_donations_made
    from `nyu-cap-ae.junk.individual_contributions_parsed_addresses`
    group by 1,2
),

--build final table with election cycle logic for donations
final as (
    select
        a.donor_id
        , d.donor_first_name
        , d.donor_last_name
        , c.donor_occupation
        , d.donor_city
        , d.donor_state
        , d.donor_zip
        , d.donor_zip4
        , d.donor_original_address
        , d.address_number
        , d.street_name_pre_directional
        , d.street_name
        , d.street_name_type
        , d.street_name_post_directional
        , d.occupancy_type
        , d.occupancy_identifer
        , d.usps_box_id
        , e.total_dollars_donated as dollars_donated_2000_cycle
        , e.total_donations_made as donations_made_2000_cycle
        , f.total_dollars_donated as dollars_donated_2002_cycle
        , f.total_donations_made as donations_made_2002_cycle
        , g.total_dollars_donated as dollars_donated_2004_cycle
        , g.total_donations_made as donations_made_2004_cycle
        , h.total_dollars_donated as dollars_donated_2006_cycle
        , h.total_donations_made as donations_made_2006_cycle
        , i.total_dollars_donated as dollars_donated_2008_cycle
        , i.total_donations_made as donations_made_2008_cycle
        , j.total_dollars_donated as dollars_donated_2010_cycle
        , j.total_donations_made as donations_made_2010_cycle
        , k.total_dollars_donated as dollars_donated_2012_cycle
        , k.total_donations_made as donations_made_2012_cycle
        , l.total_dollars_donated as dollars_donated_2014_cycle
        , l.total_donations_made as donations_made_2014_cycle
        , m.total_dollars_donated as dollars_donated_2016_cycle
        , m.total_donations_made as donations_made_2016_cycle
        , n.total_dollars_donated as dollars_donated_2018_cycle
        , n.total_donations_made as donations_made_2018_cycle
        , o.total_dollars_donated as dollars_donated_2020_cycle
        , o.total_donations_made as donations_made_2020_cycle
        , a.number_contributions as total_donations_made_all_time
        , a.total_contributions as total_dollars_all_time
        , a.republican_donations
        , a.republican_dollars
        , a.democrat_donations
        , a.democrat_dollars
        , a.other_party_dollars
        , a.other_party_donations
        , a.house_donations
        , a.house_dollars
        , a.senate_donations
        , a.senate_dollars
        , a.treasurer_donations
        , a.treasurer_dollars
        , a.secretary_of_state_donations
        , a.secretary_of_state_dollars
        , a.governor_donations
        , a.governor_dollars
        , a.auditor_donations
        , a.auditor_dollars
        , a.attorney_general_donations
        , a.attorney_general_dollars
        , a.undeclared_donations
        , a.undeclared_dollars
        , a.total_contributions - a.house_dollars - a.senate_dollars - a.treasurer_dollars - a.secretary_of_state_dollars - a.governor_dollars - a.auditor_dollars - a.attorney_general_dollars as other_dollars
        , a.number_contributions  - a.house_donations - a.senate_donations - a.treasurer_donations - a.secretary_of_state_donations - a.governor_donations - a.auditor_donations - a.attorney_general_donations as other_donations
    from
        donor_summary a
        left join (select * from donor_occupation_demo where donor_occupation_demo.row_rank = 1) c on a.donor_id = c.donor_id
        left join (select * from donor_address_demo where donor_address_demo.row_rank2 = 1) d on a.donor_id = d.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2000) group by 1) as e on a.donor_id = e.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2001,2002) group by 1) as f on a.donor_id = f.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2003,2004) group by 1) as g on a.donor_id = g.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2005,2006) group by 1) as h on a.donor_id = h.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2007,2008) group by 1) as i on a.donor_id = i.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2009,2010) group by 1) as j on a.donor_id = j.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2011,2012) group by 1) as k on a.donor_id = k.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2013,2014) group by 1) as l on a.donor_id = l.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2015,2016) group by 1) as m on a.donor_id = m.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2017,2018) group by 1) as n on a.donor_id = n.donor_id
        left join (select donor_id, sum(total_dollars_donated) as total_dollars_donated, sum(total_donations_made) as total_donations_made from donor_donations_by_year where donation_year IN (2019,2020) group by 1) as o on a.donor_id = o.donor_id
)

select *
from final;

-- Create the career for each occupational token matched
CREATE OR REPLACE TABLE `nyu-cap-ae.junk.donor_id_occ_token` as

select
    d.donor_id,
    d.donor_occupation,

    case when regexp_contains(d.donor_occupation, r'ATTORNEY') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'ATTORNEY') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'COUNCIL') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'LAW') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'COUNSEL') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'ATTY') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'PORTER WRIGHT') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'ROETZEL & ANDRESS') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'DICKINSON WRIGHT PLLC') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'JONES DAY') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'BRICKER & ECKLER') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'TAFT STETTINIUS & HOLLISTERR') = true then 'Legal'
    when regexp_contains(d.donor_occupation, r'OIL') = true then 'Energy'
    when regexp_contains(d.donor_occupation, r'GAS') = true then 'Energy'
    when regexp_contains(d.donor_occupation, r'HEATING') = true then 'Energy'
    when regexp_contains(d.donor_occupation, r'COOLING') = true then 'Energy'
    when regexp_contains(d.donor_occupation, r'FARM') = true then 'Farming'
    when regexp_contains(d.donor_occupation, r'OWNER OF DOG BOARDING KENNEL') = true then 'Petcare'
    when regexp_contains(d.donor_occupation, r'STUDENT') = true then 'Student'
    when regexp_contains(d.donor_occupation, r'TRAVEL') = true then 'Tourism'
    when regexp_contains(d.donor_occupation, r'TEACHER') = true then 'Education'
    when regexp_contains(d.donor_occupation, r'SCHOOL') = true then 'Education'
    when regexp_contains(d.donor_occupation, r'UNIVERSITY') = true then 'Education'
    when regexp_contains(d.donor_occupation, r'PROF') = true then 'Education'
    when regexp_contains(d.donor_occupation, r'GUIDANCE') = true then 'Education'
    when regexp_contains(d.donor_occupation, r'PROFESSOR') = true then 'Education'
    when regexp_contains(d.donor_occupation, r'INSUR') = true then 'Insurance'
    when regexp_contains(d.donor_occupation, r'AUTO') = true then 'Automotive'
    when regexp_contains(d.donor_occupation, r'MOTOR') = true then 'Automotive'
    when regexp_contains(d.donor_occupation, r'MUNICIPAL') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'CITY') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'COUNTY') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'GOVERN') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'TOWN') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'OHIO') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'STATE') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'DISTRICT') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'FEDERA') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'LOBBYIST') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'JUDG') = true then 'Government'
    when regexp_contains(d.donor_occupation, r'PHARMACY') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'HEALTH') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'DOCT') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'CHIRO') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'SURGER') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'NURSE') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'PHYSIC') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'DENTIST') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'OPTOM') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'COUNSELOR') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'COUNSELING') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'HOSPIT') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'CLINIC') = true then 'Healthcare'
    when regexp_contains(d.donor_occupation, r'FOUNDATION') = true then 'Non-Profit'
    when regexp_contains(d.donor_occupation, r'DEVELOPER') = true then 'Technology'
    when regexp_contains(d.donor_occupation, r'TECH') = true then 'Technology'
    when regexp_contains(d.donor_occupation, r'IT') = true then 'Technology'
    when regexp_contains(d.donor_occupation, r'PROGRAMMER') = true then 'Technology'
    when regexp_contains(d.donor_occupation, r'NOT EMPLOYED') = true then 'Unemployed'
    when regexp_contains(d.donor_occupation, r'NONE') = true then 'Unemployed'
    when regexp_contains(d.donor_occupation, r'REAL ESTATE') = true then 'Real Estate'
    when regexp_contains(d.donor_occupation, r'ESTAT') = true then 'Real Estate'
    when regexp_contains(d.donor_occupation, r'REALTY') = true then 'Real Estate'
    when regexp_contains(d.donor_occupation, r'RESTAUR') = true then 'Food Services'
    when regexp_contains(d.donor_occupation, r'GROCE') = true then 'Food Services'
    when regexp_contains(d.donor_occupation, r'FOOD') = true then 'Food Services'
    when regexp_contains(d.donor_occupation, r'HOMEMAK') = true then 'Self-Employed'
    when regexp_contains(d.donor_occupation, r'HOUSEWIFE') = true then 'Self-Employed'
    when regexp_contains(d.donor_occupation, r'SALES') = true then 'Self-Employed'
    when regexp_contains(d.donor_occupation, r'BUSINESS OWNER') = true then 'Sales (Generic)'
    when regexp_contains(d.donor_occupation, r'CPA') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'FINANC') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'AUDITOR') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'TAX') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'INVEST') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'WEALTH') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'BANK') = true then 'Finance/Accounting'
    when regexp_contains(d.donor_occupation, r'No Occupation Listed') = true then 'No Occupation Listed'
    when regexp_contains(d.donor_occupation, r'NOT REQUIRED') = true then 'No Occupation Listed'
    when regexp_contains(d.donor_occupation, r'N/A') = true then 'No Occupation Listed'
    when regexp_contains(d.donor_occupation, r'NA') = true then 'No Occupation Listed'
    when regexp_contains(d.donor_occupation, r'CONSULT') = true then 'Professional Services'
    when regexp_contains(d.donor_occupation, r'WRITER') = true then 'Professional Services'
    when regexp_contains(d.donor_occupation, r'THERAPIST') = true then 'Professional Services'
    when regexp_contains(d.donor_occupation, r'RETIR') = true then 'Self-Employed/Retired'
    when regexp_contains(d.donor_occupation, r'SELF') = true then 'Self-Employed/Retired'
    when regexp_contains(d.donor_occupation, r'ARTIST') = true then 'Self-Employed/Retired'
    when regexp_contains(d.donor_occupation, r'NOT EMPLOYED/RETIRED') = true then 'Self-Employed/Retired'
    when regexp_contains(d.donor_occupation, r'LUMBER') = true then 'Industrial/Manufacturing'
    when regexp_contains(d.donor_occupation, r'ELECTR') = true then 'Industrial/Manufacturing'
    when regexp_contains(d.donor_occupation, r'METAL') = true then 'Industrial/Manufacturing'
    when regexp_contains(d.donor_occupation, r'MACHINE') = true then 'Industrial/Manufacturing'
    when regexp_contains(d.donor_occupation, r'MANUFAC') = true then 'Industrial/Manufacturing'
    when regexp_contains(d.donor_occupation, r'ENGINEER') = true then 'Engineering, Design, Architecture'
    when regexp_contains(d.donor_occupation, r'ARCHITECT') = true then 'Engineering, Design, Architecture'

    else 'UNK' end as token

from nyu-cap-ae.elt_ohio.donors as d;

-- Create final donors table
create or replace table `nyu-cap-ae.elt_ohio.donors` as

select
    a.donor_id
    , a.donor_first_name
    , a.donor_last_name
    , a.donor_occupation
    , b.token as donor_profession
    , a.donor_city
    , a.donor_state
    , a.donor_zip
    , a.donor_zip4
    , a.donor_original_address
    , a.address_number
    , a.street_name_pre_directional
    , a.street_name
    , a.street_name_type
    , a.street_name_post_directional
    , a.occupancy_type
    , a.occupancy_identifer
    , a.usps_box_id
    , a.dollars_donated_2000_cycle
    , a.donations_made_2000_cycle
    , a.dollars_donated_2002_cycle
    , a.donations_made_2002_cycle
    , a.dollars_donated_2004_cycle
    , a.donations_made_2004_cycle
    , a.dollars_donated_2006_cycle
    , a.donations_made_2006_cycle
    , a.dollars_donated_2008_cycle
    , a.donations_made_2008_cycle
    , a.dollars_donated_2010_cycle
    , a.donations_made_2010_cycle
    , a.dollars_donated_2012_cycle
    , a.donations_made_2012_cycle
    , a.dollars_donated_2014_cycle
    , a.donations_made_2014_cycle
    , a.dollars_donated_2016_cycle
    , a.donations_made_2016_cycle
    , a.dollars_donated_2018_cycle
    , a.donations_made_2018_cycle
    , a.dollars_donated_2020_cycle
    , a.donations_made_2020_cycle
    , a.total_donations_made_all_time
    , a.total_dollars_all_time
    , a.republican_donations
    , a.republican_dollars
    , a.democrat_donations
    , a.democrat_dollars
    , a.other_party_dollars
    , a.other_party_donations
    , a.house_donations
    , a.house_dollars
    , a.senate_donations
    , a.senate_dollars
    , a.treasurer_donations
    , a.treasurer_dollars
    , a.secretary_of_state_donations
    , a.secretary_of_state_dollars
    , a.governor_donations
    , a.governor_dollars
    , a.auditor_donations
    , a.auditor_dollars
    , a.attorney_general_donations
    , a.attorney_general_dollars
    , a.undeclared_donations
    , a.undeclared_dollars
    , a.other_dollars
    , a.other_donations
from `nyu-cap-ae.elt_ohio.donors` a
inner join `nyu-cap-ae.junk.donor_id_occ_token` b on a.donor_id = b.donor_id;
