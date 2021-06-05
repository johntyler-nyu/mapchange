/**
Data needed:
Report Year of Donation
Donor ID
Donor State
Donor ZIP
Donor Career
Donor / Voterfile Year of Birth
Contribution Amount
Candidate Party
Candidate Office
FIPs, state, countyname, stcountyfp **revisit
**/

with donor_demo as (
    select
        a.donor_id
        , MAX(donor_state) as donor_state
        , MAX(donor_zip) as donor_zip
        , MAX(career) as donor_career
        , MAX(CAST(LEFT(contribution_year, 4) AS NUMERIC))
    from `nyu-cap-ae.elt_ohio.donors` a
    inner join `nyu-cap-ae.elt_ohio.contributions` b on a.donor_id = b.donor_id
    group by 1)

select
    a.contribution_id
    , a.donor_id
    , b.donor_state
    , b.donor_zip
    , b.donor_career
    , DATE_DIFF(current_date(), d.date_of_birth, year) as donor_age
    , a.contribution_amount
    , a.contribution_year
    , c.candidate_party
    , c.candidate_office
    , e.zip as fip_zip
    , e.state as fip_state
    , e.COUNTYNAME as fip_countyname
    , e.STCOUNTYFP as fip_stcountyfp
from
`nyu-cap-ae.elt_ohio.contributions` a
left outer join donor_demo b on b.donor_id = a.donor_id
left outer join (select election_id, candidate_id, candidate_party, candidate_office from `nyu-cap-ae.elt_ohio.elections_candidates` group by 1,2,3,4) c
    on (c.election_id = a.election_id AND c.candidate_id = a.candidate_id)
left outer join (select donor_id, min(DATE_OF_BIRTH) as date_of_birth from `nyu-cap-ae.elt_ohio.voterfiles` group by 1) d on d.donor_id = a.donor_id
left outer join (select zip, state, COUNTYNAME, STCOUNTYFP from `nyu-cap-ae.junk.tbl_tableaugroup_fips`) e on b.donor_zip = cast(e.ZIP as string);
