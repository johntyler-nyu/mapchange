with test as (select  
    *, LEFT(donor_first_name,1)||donor_last_name||donor_zip||left(donor_city, 4)||left(donor_original_address,2) as donor_id
from `nyu-cap-ae.junk.individual_contributions_parsed_addresses` as a
left join `nyu-cap-ae.elt_ohio.elections_candidates` as b on (
    a.candidate_first_name=b.candidate_first_name
    and a.candidate_last_name=b.candidate_last_name 
    and a.candidate_party=b.candidate_party 
    and a.candidate_office=b.candidate_office 
    and a.office_district=b.office_district	
    and LEFT(report_year,4)=b.donation_year
    and a.committee_name=b.committee_name)
)

select 
    contribution_id
    , donor_id 
    , candidate_id
    , election_id
    , contribution_amount
    , contribution_date
    , CAST(LEFT(report_year,4) AS STRING) as contribution_year
from test