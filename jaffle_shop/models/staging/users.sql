{{
    config(materialized='table_with_connector')
}}

CREATE TABLE users (
    user_id INT PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(200),
    registration_date TIMESTAMP,
    user_profile JSONB,  -- Contains nested user information
    is_active BOOLEAN DEFAULT true
) FROM user_events_cdc TABLE 'public.users'
