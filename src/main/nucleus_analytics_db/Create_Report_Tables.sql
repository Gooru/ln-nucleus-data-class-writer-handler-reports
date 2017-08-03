-- You can run this command using the newly created nucleus user:
-- psql -U nucleus -f <path to>/Create_ClassReports_Table.sql
-- This may have to check-in seperate repository. But initially let it be here. We can move this later.

-- Resource Attempt status
CREATE TYPE attempt_status AS ENUM ('correct', 'incorrect', 'skipped', 'unevaluated', 'started');


-- Store data for Class Analytics 
CREATE TABLE base_reports (
id SERIAL PRIMARY KEY,
sequence_id bigint,
event_name varchar(36) NOT NULL,
event_type varchar(36) NOT NULL,
actor_id varchar(36) NOT NULL,
tenant_id varchar(36) DEFAULT NULL,
class_id varchar(36) DEFAULT NULL,
course_id varchar(36) DEFAULT NULL,
unit_id varchar(36) DEFAULT NULL,
lesson_id varchar(36) DEFAULT NULL,
collection_id varchar(36) NOT NULL,
session_id varchar(36),
question_count smallint,
collection_type varchar(12),
resource_type varchar(12),
question_type varchar(36),
answer_object text, 
resource_id varchar(36),
views integer,
time_spent bigint,
score smallint,
reaction smallint,
resource_attempt_status attempt_status,
created_timestamp timestamp NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
updated_timestamp timestamp NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'));

--Store data for taxonomy report
CREATE TABLE taxonomy_report (
id SERIAL PRIMARY KEY,
sequence_id bigint,
session_id varchar(36),
actor_id varchar(36) NOT NULL,
tenant_id varchar(36) DEFAULT NULL,
subject_id varchar(36) NOT NULL,
course_id varchar(36) DEFAULT NULL,
domain_id varchar(36) DEFAULT NULL,
standard_id varchar(36) DEFAULT NULL,
learning_target_id varchar(36) DEFAULT NULL,
display_code varchar(36) NOT NULL,
collection_id varchar(36) NOT NULL,
resource_id varchar(36),
resource_type varchar(36),
question_type varchar(12),
answer_object text, 
resource_attempt_status varchar(36),
views smallint,
reaction smallint,
score smallint,
time_spent bigint);

-- Store Class Lookup data for Analytics 
CREATE TABLE course_collection_count (
course_id varchar(36) NOT NULL,
unit_id varchar(36) NOT NULL,
lesson_id varchar(36) NOT NULL,
collection_count integer,
assessment_count integer,
ext_assessment_count integer,
created_at timestamp NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
updated_at timestamp NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
PRIMARY KEY(course_id,unit_id,lesson_id)
);

--Create class_authorized_users
CREATE TABLE class_authorized_users (
 class_id varchar(36) NOT NULL,
 creator_id varchar(36) NOT NULL, 
 collaborator_id varchar(36) DEFAULT NULL, 
 modified timestamp NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
 PRIMARY KEY (class_id) 
);


