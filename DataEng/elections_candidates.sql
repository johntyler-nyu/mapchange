with candidate_temp as (
    select  
        candidate_first_name
        , committee_name
        , candidate_last_name
        , candidate_party
        , candidate_office
        , CASE WHEN office_district = 'NA' THEN "" ELSE office_district END as office_district
        , cast(left(report_year,4) as string) as donation_year
        , left(candidate_first_name,2)||left(candidate_last_name,1)||right(candidate_last_name,1)||left(candidate_party,1) as candidate_id
    from `nyu-cap-ae.junk.individual_contributions_parsed_addresses`
    group by 1,2,3,4,5,6,7,8
),

 election_years as (
    select 
        seat
        , district
        , year
        , election_id
        , running_year
    from 
        `nyu-cap-ae.elt_ohio.elections`
    cross join unnest(running_years) as running_year
)

select 
    candidate_id
    , election_id
    , committee_name
    , candidate_first_name
    , candidate_last_name
    , candidate_party 
    , candidate_office
    , office_district
    , donation_year
    , year as election_year
from candidate_temp a
inner join election_years b on (a.donation_year = b.running_year AND a.candidate_office = b.seat AND a.office_district = CAST(b.district AS STRING))



/**

create a table of unique candidates
===================================
from donation records... 
- a candidate reports donations any year s/he is running for office 
- each year belongs to a cycle for a given race
- if the office and report_year match a running year, that's the associated election id // signifies a unique candidacy 


**/