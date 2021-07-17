/** uses voterfiles table loaded from DataPrep and adds the donor_id created in the donor table to join
the two datasets together **/

CREATE OR REPLACE TABLE `nyu-cap-ae.elt_ohio.voterfiles`
as
select *
,LEFT(FIRST_NAME,3)||LEFT(LAST_NAME,4)||RESIDENTIAL_ZIP||LEFT(RESIDENTIAL_CITY,1)||LEFT(RESIDENTIAL_ADDRESS1,2) as donor_id
from `nyu-cap-ae.junk.voterfiles_dataprep_20210717_205010`;

delete from `nyu-cap-ae.elt_ohio.voterfiles` where donor_id is null;
