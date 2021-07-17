/** source table is created from google drive sheet in shared MapChange drive
sheet contains seat, district, year of each election in Ohio for the following races:
- House
- Senate
- Gubernatorial 
- Secretary of State
- Auditor
- Treasurer 
- Attorney General

It also includes the "running years", or years that a candidate might be running for 
that office and\or collecting/reporting campaign contributions to their candidacy. **/ 

select 
    seat
    , district
    , year
    , election_id
    , array(select*from unnest(split(running_years, " "))) as running_years
from `nyu-cap-ae.junk.elections`