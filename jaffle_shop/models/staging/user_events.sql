{{
    config(materialized='table_with_connector')
}}

CREATE TABLE user_events (
    event_id VARCHAR,
    user_id INT,
    event_type VARCHAR,
    event_data JSONB,  -- This contains nested keys
    event_timestamp TIMESTAMP,
    user_properties JSONB,  -- Additional nested data
    PRIMARY KEY (event_id, event_timestamp)
) FROM user_events_cdc TABLE 'public.user_events'
