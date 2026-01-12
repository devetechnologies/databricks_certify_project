SET school.dataset_path=dbfs:/mnt/DE-Associate-Book/datasets/school;

--drop table enrollments_cleaned;
--drop table students_dlt;
--drop table uk_daily_student_courses;
--drop table enrollments_raw;

create or refresh streaming table enrollments_raw
comment "The raw courses enrollments, ingested from enrollments-dlt-raw folder"
as
select * from cloud_files("${school.dataset_path}/enrollments-dlt-raw", 
'json', 
map('cloudFiles.inferColumnTypes',"true"));



CREATE OR replace MATERIALIZED VIEW students_dlt
COMMENT "The students lookup table, ingested from students-json"
AS SELECT * FROM json.`${school.dataset_path}/students-json`;

create or refresh streaming table enrollments_cleaned (
  constraint valid_order_number expect (enroll_id is not null ) on violation drop row
)
comment "The cleaned courses enrollments"
 as
  select 
  enroll_id,quantity,
  o.student_id,
  c.profile:first_name as f_name,
  c.profile:last_name as l_name,
  cast(from_unixtime(enroll_timestamp,'yyyy-MM-dd HH:mm:ss') as timestamp) as formatted_timestamp,
  o.courses,
  c.profile:address:country as country
  from stream(Live.enrollments_raw) o
  left join Live.students_dlt c
    on o.student_id = c.student_id;

CREATE or replace MATERIALIZED VIEW  uk_daily_student_courses
COMMENT "Daily number of courses per student in United Kingdom"
AS
 SELECT student_id, f_name, l_name,
        date_trunc("DD", formatted_timestamp) order_date,
        sum(quantity) courses_counts
 FROM LIVE.enrollments_cleaned
 WHERE country = "United Kingdom"
 GROUP BY student_id, f_name, l_name, date_trunc("DD", formatted_timestamp)
 order by courses_counts;

 CREATE or replace MATERIALIZED VIEW  fr_daily_student_courses
  COMMENT "Daily number of courses per student in France"
  AS
  SELECT student_id, f_name, l_name,
          date_trunc("DD", formatted_timestamp) order_date,
          sum(quantity) courses_counts
  FROM LIVE.enrollments_cleaned
  WHERE country = "France"
  GROUP BY student_id, f_name, l_name, date_trunc("DD", formatted_timestamp)
  order by courses_counts