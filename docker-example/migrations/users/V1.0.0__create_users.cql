-- V1.0.0__create_users.cql
-- Initial schema: create the users table

CREATE TABLE IF NOT EXISTS users (
    user_id    uuid PRIMARY KEY,
    email      text,
    username   text,
    created_at timestamp
);

CREATE INDEX IF NOT EXISTS ON users (email);
