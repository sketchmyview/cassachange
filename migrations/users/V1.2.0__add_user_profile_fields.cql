-- V1.2.0__add_user_profile_fields.cql
-- Cassandra supports adding columns without downtime

ALTER TABLE users ADD full_name text;
ALTER TABLE users ADD avatar_url text;
ALTER TABLE users ADD last_login timestamp;
