select 
    candidate_id
    , committee_name
    , candidate_first_name
    , candidate_last_name
    , candidate_party
    , candidate_office
    , office_district
    , election_year 
from `nyu-cap-ae.elt_ohio.elections_candidates` 
group by 1,2,3,4,5,6,7,8;