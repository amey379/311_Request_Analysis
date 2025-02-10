select * from boston_311_stage;
select count(*) from boston_311_stage;-- 2585924 
select * from lkp_police_districts;
select * from lkp_fire_districts;
select * from lkp_pwd_districts;
select * from lkp_city_council_districts;
select * from  lkp_electoral_divisions;
select * from dim_date;
select * from dim_time;
select * from dim_source;
select * from dim_request_dtl;
select count(*) from dim_request_dtl;-- 43891
select * from dim_location;
select count(*) from dim_location;-- 146309
select * from fact_311_request; 
select count(*) from fact_311_request;-- 2585924
