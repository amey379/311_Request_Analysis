CREATE TABLE `boston_311_stage` (
  `bos_case_id` int NOT NULL AUTO_INCREMENT,
  `case_enquiry_id` bigint,
  `open_dt` datetime DEFAULT NULL,
  `sla_target_dt` datetime DEFAULT NULL,
  `closed_dt` datetime DEFAULT NULL,
  `on_time` varchar(10) DEFAULT NULL,
  `case_status` varchar(10) DEFAULT NULL,
  `closure_reason` varchar(255) DEFAULT NULL,
  `case_title` varchar(255) DEFAULT NULL,
  `subject` varchar(100) DEFAULT NULL,
  `reason` varchar(50) DEFAULT NULL,
  `type` varchar(50) DEFAULT NULL,
  `queue` varchar(60) DEFAULT NULL,
  `department` varchar(4) DEFAULT NULL,
  `location` varchar(110) DEFAULT NULL,
  `fire_district` varchar(5) DEFAULT NULL,
  `pwd_district` varchar(5) DEFAULT NULL,
  `city_council_district` varchar(1) DEFAULT NULL,
  `police_district` varchar(5) DEFAULT NULL,
  `neighborhood` varchar(50) DEFAULT NULL,
  `neighborhood_services_district` varchar(2) DEFAULT NULL,
  `ward` varchar(10) DEFAULT NULL,
  `precinct` varchar(5) DEFAULT NULL,
  `location_street_name` varchar(80) DEFAULT NULL,
  `location_zipcode` varchar(6) DEFAULT NULL,
  `latitude` decimal(10,7) DEFAULT NULL,
  `longitude` decimal(10,7) DEFAULT NULL,
  `geom_4326` text,
  `source` varchar(20) DEFAULT NULL,
  `db_created_datetime` datetime DEFAULT CURRENT_TIMESTAMP,
  `db_modified_datetime` datetime DEFAULT NULL,
  `created_by` varchar(20) DEFAULT 'system',
  `modified_by` varchar(20) DEFAULT 'system',
  `process_id` int DEFAULT '1',
  PRIMARY KEY (`bos_case_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2585925 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE INDEX idx_case_enquiry_id ON boston_311_stage (case_enquiry_id);

ALTER TABLE boston_311_stage 
MODIFY COLUMN db_modified_datetime DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;


CREATE TABLE dim_time (
    time_key INT NOT NULL ,            -- Integer representation of time in HHmmss format
    time_key_str VARCHAR(6) NOT NULL, -- String representation of time in HHmmss format
    time TIME NOT NULL,               -- Time in HH:MM:SS format
    hour INT NOT NULL,                -- Hour part of the time
    minute INT NOT NULL,              -- Minute part of the time
    second INT NOT NULL,              -- Second part of the time
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(255) DEFAULT 'system',
    modified_by VARCHAR(255) DEFAULT 'system',
    process_id INT DEFAULT 1,
	PRIMARY KEY (time_key)           -- Primary key on time_key
);
drop table dim_date;
CREATE TABLE dim_date (
    date_key INT NOT NULL,            -- Integer representation of date in YYYYMMDD format
    date_key_str VARCHAR(8) NOT NULL, -- String representation of date in YYYYMMDD format
    date DATE NOT NULL,               -- Date in YYYY-MM-DD format
    year INT NOT NULL,                -- Year as an integer
    month INT NOT NULL,               -- Month as an integer (1-12)
    day_of_month INT NOT NULL,        -- Day of the month as an integer (1-31)
    day_of_week INT NOT NULL,         -- Day of the week as an integer (1=Sunday, 7=Saturday)
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(255) DEFAULT 'system',
    modified_by VARCHAR(255) DEFAULT 'system',
    process_id INT DEFAULT 1,
    PRIMARY KEY (date_key)            -- Primary key on date_key
);


drop table dim_police_districts;
CREATE TABLE lkp_police_districts (
    police_district VARCHAR(3) primary key NOT NULL,   -- District code (e.g., A1, A7)
    name VARCHAR(15) NOT NULL,            -- District name (e.g., Downtown, Roxbury)
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20) ,
    process_id INT DEFAULT 1
);

drop table dim_fire_districts;
CREATE TABLE lkp_fire_districts (
    fire_district int  primary key NOT NULL,   -- District code (e.g., A1, A7)
    division varchar(10),
    name VARCHAR(100) NOT NULL,            -- District name (e.g., Downtown, Roxbury)
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1
);

drop table dim_pwd_districts;
CREATE TABLE lkp_pwd_districts (
    pwd_district varchar(3)  primary key NOT NULL,   -- District code (e.g., A1, A7)
    name VARCHAR(50) NOT NULL,            -- District name (e.g., Downtown, Roxbury)
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1
);

drop table dim_city_council_districts;
CREATE TABLE lkp_city_council_districts (
    city_council_district int  primary key NOT NULL,   -- District code (e.g., A1, A7)
    name VARCHAR(50) NOT NULL,            -- District name (e.g., Downtown, Roxbury)
    councilor VARCHAR(20) NOT NULL,
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1
);

drop table dim_electoral_divisions;
CREATE TABLE lkp_electoral_divisions (
    wards int  NOT NULL,   -- District code (e.g., A1, A7)
	precinct int  NOT NULL,
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1,
    Primary Key(wards, precinct)
);


CREATE TABLE dim_source (
    source_key int  NOT NULL,  
	source varchar(20)   NOT NULL,
	db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1,
    Primary Key(source_key)
);

CREATE TABLE dim_request_dtl (
    request_dtl_key INT AUTO_INCREMENT PRIMARY KEY,
    case_title VARCHAR(255) NOT NULL,
    subject VARCHAR(100) NOT NULL,
    reason VARCHAR(50) NOT NULL,
    type VARCHAR(50) NOT NULL,
    department VARCHAR(4) NOT NULL,
    queue VARCHAR(60) NOT NULL,
    db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1
);

drop table dim_location;
CREATE TABLE dim_location (
    location_key INT AUTO_INCREMENT PRIMARY KEY,
    location_street_name varchar(80) DEFAULT NULL,
    is_intersection ENUM('Y', 'N') NOT NULL,
    street_name1 VARCHAR(100) NOT NULL,
    street_name2 VARCHAR(100),
    neighborhood VARCHAR(50),
    city VARCHAR(10) DEFAULT 'Boston',
    state VARCHAR(2) DEFAULT 'MA',
    zipcode VARCHAR(6),
    neighborhood_services_district INT,
    police_district VARCHAR(3),
    fire_district INT,
    pwd_district VARCHAR(3),
    city_council_district INT,
    ward INT,
    precinct INT,
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1
);
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_1;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_2;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_3;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_4;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_5;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_6;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_7;
ALTER TABLE fact_311_request DROP FOREIGN KEY fact_311_request_ibfk_8;

drop table fact_311_request;
CREATE TABLE fact_311_request (
    request_id INT AUTO_INCREMENT PRIMARY KEY,
    case_stg_id INT NOT NULL,
    case_enquiry_id VARCHAR(50) NOT NULL,
    open_date DATETIME,
    sla_target_date DATETIME,
    closed_date DATETIME,
    open_date_key INT,
    open_time_key INT,
    sla_date_key INT,
    sla_time_key INT,
    close_date_key INT,
    close_time_key INT,
    request_dtl_key INT,
    source_key INT,
    location_key INT,
    closure_reason VARCHAR(255),
    case_status VARCHAR(50),
    on_time VARCHAR(10),
    db_created_datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_modified_datetime DATETIME,
    created_by VARCHAR(20) DEFAULT 'system',
    modified_by VARCHAR(20),
    process_id INT DEFAULT 1
);

INSERT INTO dim_date (date_key, date_key_str, date, year, month, day_of_month, day_of_week) 
VALUES (-1, '21240101', '2124-01-01', 2124, 1, 1, 2);

INSERT INTO dim_time (time_key, time_key_str, time, hour, minute, second) 
VALUES (-1, '000000', '00:00:00', 0, 0, 0);

INSERT INTO dim_location (location_key, is_intersection, street_name1, city, state) 
VALUES (-1, 'N', 'UNKNOWN', 'Boston', 'MA');

INSERT INTO dim_request_dtl (request_dtl_key, case_title, subject, reason, type, department, queue)
VALUES (-1, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNK', 'UNKNOWN');

INSERT INTO dim_source (source_key, source)
VALUES (-1, 'UNKNOWN');

CREATE INDEX idx_fact_request_dtl ON fact_311_request (request_dtl_key);
CREATE INDEX idx_fact_source ON fact_311_request (source_key);
CREATE INDEX idx_fact_location ON fact_311_request (location_key);
CREATE INDEX idx_fact_open_date ON fact_311_request (open_date_key);
CREATE INDEX idx_fact_sla_date ON fact_311_request (sla_date_key);
CREATE INDEX idx_fact_close_date ON fact_311_request (close_date_key);