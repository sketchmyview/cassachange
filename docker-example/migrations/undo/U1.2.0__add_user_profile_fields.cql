-- U1.2.0__add_user_profile_fields.cql
-- Undo script for V1.2.0 - removes the profile columns added

ALTER TABLE users DROP full_name;
ALTER TABLE users DROP avatar_url;
ALTER TABLE users DROP last_login;
