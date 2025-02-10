truncate boston_311_stage;
drop index idx_case_enquiry_id ON boston_311_stage;
CREATE INDEX idx_case_enquiry_id ON boston_311_stage (case_enquiry_id);


truncate lkp_police_districts;
truncate lkp_fire_districts;
truncate lkp_pwd_districts;
truncate lkp_city_council_districts;
truncate  lkp_electoral_divisions;


truncate dim_date;
truncate  dim_time;
truncate  dim_source;
truncate  dim_request_dtl;
truncate dim_location;

truncate fact_311_request;